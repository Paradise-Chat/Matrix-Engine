(ns paradise.engine.matrix.call
  (:require [cljs.core.async :refer [go <! promise-chan put! take! timeout]]
            [cljs.core.async.interop :refer-macros [<p!]]
            [clojure.set :as set]
            [paradise.engine.binding :as bind]
            [cljs-workers.worker :as worker]
            [paradise.engine.state :as state]
            [clojure.string :as str]
            [taoensso.timbre :as log]
            ["ffi-bindings" :as sdk]))

(defonce !widget-handle (atom nil))
(defonce !widget-driver (atom nil))
(defonce !active-widget-id (atom nil))
(defonce !widget-generation (atom 0))
(defonce !rpc-callbacks (atom {}))
(defonce !local-key-index (atom 0))
(defonce !local-key-material (atom nil))
(defonce !local-key-creation-ts (atom 0))
(defonce !shared-with-users (atom #{}))

(defonce !widget-run-promise (atom nil))
(defonce !ffi-send-queue (atom (js/Array.)))
(defonce !ffi-send-pumping? (atom false))
(defonce !ffi-debug-interval (atom nil))

(defonce !ffi-run-started (atom 0))
(defonce !ffi-run-finished (atom 0))
(defonce !ffi-run-failed (atom 0))
(defonce !ffi-send-started (atom 0))
(defonce !ffi-send-finished (atom 0))
(defonce !ffi-send-failed (atom 0))
(defonce !ffi-send-dropped (atom 0))
(defonce !ffi-recv-started (atom 0))
(defonce !ffi-recv-finished (atom 0))
(defonce !ffi-recv-failed (atom 0))
(defonce !ffi-recv-null (atom 0))
(defonce !ffi-recv-messages (atom 0))

(def key-rotation-participant-limit 50)
(def key-rotation-grace-period-ms 10000)
(def rpc-connect-retry-count 50)
(def rpc-connect-retry-delay-ms 100)
(def rpc-response-timeout-ms 15000)
(def ffi-send-queue-limit 256)
(def ffi-debug-log-interval-ms 5000)

(defn- widget-active?
  [generation handle]
  (and (= generation @!widget-generation)
       (identical? handle @!widget-handle)))

(defn- reset-ffi-debug-counters!
  []
  (reset! !ffi-run-started 0)
  (reset! !ffi-run-finished 0)
  (reset! !ffi-run-failed 0)
  (reset! !ffi-send-started 0)
  (reset! !ffi-send-finished 0)
  (reset! !ffi-send-failed 0)
  (reset! !ffi-send-dropped 0)
  (reset! !ffi-recv-started 0)
  (reset! !ffi-recv-finished 0)
  (reset! !ffi-recv-failed 0)
  (reset! !ffi-recv-null 0)
  (reset! !ffi-recv-messages 0))

(defn- stop-ffi-debug-logger!
  []
  (when-let [interval-id @!ffi-debug-interval]
    (js/clearInterval interval-id)
    (reset! !ffi-debug-interval nil)))

(defn- start-ffi-debug-logger!
  [generation handle]
  (stop-ffi-debug-logger!)
  (let [interval-id
        (js/setInterval
         (fn []
           (when (widget-active? generation handle)
             (let [queue @!ffi-send-queue
                   run-pending  (- @!ffi-run-started @!ffi-run-finished)
                   send-pending (- @!ffi-send-started @!ffi-send-finished)
                   recv-pending (- @!ffi-recv-started @!ffi-recv-finished)
                   memory       (some-> js/performance (aget "memory"))
                   heap-mb      (when memory
                                  (/ (aget memory "usedJSHeapSize")
                                     1048576))]
               (log/info
                "[MatrixRTC FFI]"
                {:generation generation
                 :run-pending run-pending
                 :run-started @!ffi-run-started
                 :run-finished @!ffi-run-finished
                 :run-failed @!ffi-run-failed
                 :send-pending send-pending
                 :send-queued (.-length queue)
                 :send-started @!ffi-send-started
                 :send-finished @!ffi-send-finished
                 :send-failed @!ffi-send-failed
                 :send-dropped @!ffi-send-dropped
                 :recv-pending recv-pending
                 :recv-started @!ffi-recv-started
                 :recv-finished @!ffi-recv-finished
                 :recv-failed @!ffi-recv-failed
                 :recv-null @!ffi-recv-null
                 :recv-messages @!ffi-recv-messages
                 :js-heap-mb heap-mb}))))
         ffi-debug-log-interval-ms)]
    (reset! !ffi-debug-interval interval-id)))

(defn- resolve-queued-send!
  [item value]
  (when-let [resolve (and item (aget item "resolve"))]
    (try
      (resolve value)
      (catch :default _))))

(defn- clear-ffi-send-queue!
  []
  (let [queue @!ffi-send-queue]
    (loop []
      (when-let [item (.shift queue)]
        (resolve-queued-send! item false)
        (recur))))
  (reset! !ffi-send-queue (js/Array.))
  (reset! !ffi-send-pumping? false))

(declare pump-ffi-send-queue!)

(defn- finish-ffi-send!
  [item success? err]
  (let [generation (aget item "generation")
        handle     (aget item "handle")
        current?   (= generation @!widget-generation)]
    (when current?
      (swap! !ffi-send-finished inc)
      (when-not success?
        (swap! !ffi-send-failed inc)))
    (when (and err current?)
      (log/warn "Widget FFI send failed:" err))
    (resolve-queued-send! item success?)

    (when (widget-active? generation handle)
      (reset! !ffi-send-pumping? false)
      (pump-ffi-send-queue!))))

(defn- pump-ffi-send-queue!
  []
  (when-not @!ffi-send-pumping?
    (reset! !ffi-send-pumping? true)
    (let [queue @!ffi-send-queue
          item  (.shift queue)]
      (if-not item
        (reset! !ffi-send-pumping? false)
        (let [generation (aget item "generation")
              handle     (aget item "handle")
              payload    (aget item "payload")]
          (if-not (widget-active? generation handle)
            (do
              (when (= generation @!widget-generation)
                (swap! !ffi-send-dropped inc))
              (resolve-queued-send! item false)
              (reset! !ffi-send-pumping? false)
              (pump-ffi-send-queue!))
            (do
              (swap! !ffi-send-started inc)
              (log/info "[MatrixRTC widget -> Rust]"
                        {:api        (aget payload "api")
                         :action     (aget payload "action")
                         :request-id (aget payload "requestId")
                         :type       (some-> payload (aget "data") (aget "type"))
                         :response?  (boolean (aget payload "response"))})
              (try
                (-> (js/Promise.resolve
                     (.send handle (js/JSON.stringify payload)))
                    (.then
                     (fn [result]
                       (finish-ffi-send! item (not= false result) nil))
                     (fn [err]
                       (finish-ffi-send! item false err))))
                (catch :default e
                  (finish-ffi-send! item false e))))))))))

(defn- queue-widget-send!
  [generation handle payload]
  (when (widget-active? generation handle)
    (let [queue @!ffi-send-queue]
      (if (>= (.-length queue) ffi-send-queue-limit)
        (do
          (swap! !ffi-send-dropped inc)
          (log/error "MatrixRTC FFI send queue full; refusing widget message"
                     {:queued (.-length queue)
                      :limit ffi-send-queue-limit
                      :action (aget payload "action")})
          nil)
        (js/Promise.
         (fn [resolve _reject]
           (.push queue #js {"generation" generation
                             "handle" handle
                             "payload" payload
                             "resolve" resolve})
           (pump-ffi-send-queue!)))))))

(defn- safe-send!
  [handle payload]
  (when handle
    (queue-widget-send! @!widget-generation handle payload)))

(defn- call-method!
  [obj method-name]
  (when obj
    (when-let [f (aget obj method-name)]
      (when (fn? f)
        (try
          (.call f obj)
          true
          (catch :default e
            (log/debug "Ignoring widget cleanup failure for" method-name ":" e)
            false))))))

(defn- call-first-method!
  [obj method-names]
  (some #(call-method! obj %) method-names))

(defn- take-rpc-callback!
  [req-id]
  (when-let [callback (get @!rpc-callbacks req-id)]
    (swap! !rpc-callbacks dissoc req-id)
    callback))

(defn- resolve-rpc!
  [req-id value]
  (when-let [{:keys [chan]} (take-rpc-callback! req-id)]
    (put! chan value)
    true))

(defn- fail-all-rpcs!
  [reason]
  (let [callbacks @!rpc-callbacks]
    (reset! !rpc-callbacks {})
    (doseq [[_ {:keys [chan]}] callbacks]
      (put! chan #js {"error" reason}))))

(defn- stop-widget-runtime!
  []
  (swap! !widget-generation inc)
  (let [driver @!widget-driver
        handle @!widget-handle]
    (reset! !widget-driver nil)
    (reset! !widget-handle nil)
    (reset! !active-widget-id nil)
    (reset! !widget-run-promise nil)
    (stop-ffi-debug-logger!)
    (clear-ffi-send-queue!)
    (fail-all-rpcs! "widget_stopped")

    (call-first-method! driver ["stop" "close" "dispose" "destroy"])
    (call-first-method! handle ["close" "stop" "dispose" "destroy"])
    (call-method! driver "free")
    (call-method! handle "free"))
  nil)

(defn dispatch-widget-req! [action data]
  (let [c      (promise-chan)
        req-id (str "req-" (random-uuid))]
    (swap! !rpc-callbacks assoc req-id {:chan c :action action})
    (go
      (loop [retries rpc-connect-retry-count]
        (if-let [handle @!widget-handle]
          (let [generation @!widget-generation
                widget-id  @!active-widget-id
                msg        #js {"api"       "fromWidget"
                                "widgetId"  widget-id
                                "requestId" req-id
                                "action"    action
                                "data"      data}]
            (if (and widget-id
                     (widget-active? generation handle)
                     (safe-send! handle msg))
              (go
                (<! (timeout rpc-response-timeout-ms))
                (when (get @!rpc-callbacks req-id)
                  (log/warn "Widget RPC timed out:" action req-id)
                  (resolve-rpc! req-id #js {"error" "timeout"})))
              (do
                (log/error "Cannot dispatch widget RPC:" action)
                (resolve-rpc! req-id #js {"error" "send_failed"}))))
          (if (pos? retries)
            (do
              (<! (timeout rpc-connect-retry-delay-ms))
              (recur (dec retries)))
            (do
              (log/error "Cannot dispatch:" action "- Widget never initialized")
              (resolve-rpc! req-id #js {"error" "timeout"}))))))
    c))

(def delayed-leave-timeout-ms 8000)
(def delayed-leave-heartbeat-ms 5000)

(defonce !delayed-leave-ids (atom {}))
(defonce !delayed-leave-heartbeat (atom nil))
(defonce !delayed-leave-heartbeat-in-flight? (atom false))

(defn- stop-delayed-leave-heartbeat!
  []
  (when-let [interval-id @!delayed-leave-heartbeat]
    (js/clearInterval interval-id)
    (reset! !delayed-leave-heartbeat nil))
  (reset! !delayed-leave-heartbeat-in-flight? false))

(defn- widget-rpc-promise!
  [action data]
  (js/Promise.
   (fn [resolve reject]
     (take! (dispatch-widget-req! action data)
            (fn [response]
              (let [error (when response (aget response "error"))]
                (cond
                  (nil? response)
                  (reject (js/Error. (str "Widget RPC returned no response: " action)))

                  error
                  (let [message
                        (if (string? error)
                          error
                          (try
                            (js/JSON.stringify error)
                            (catch :default _
                              (str error))))]
                    (reject (js/Error. (str "Widget RPC " action " failed: " message))))

                  :else
                  (resolve response))))))))

(defn- delayed-state-event!
  [_room-id event-type state-key]
  (widget-rpc-promise!
   "send_event"
   #js {"type"      event-type
        "state_key" state-key
        "content"   #js {}
        "delay"     delayed-leave-timeout-ms}))

(defn- update-delayed-event!
  [delay-id action]
  (widget-rpc-promise!
   "org.matrix.msc4157.update_delayed_event"
   #js {"delay_id" delay-id
        "action"   action}))

(defn- cancel-delayed-leaves!
  []
  (let [entries (seq @!delayed-leave-ids)]
    (if-not entries
      (js/Promise.resolve true)
      (let [promises
            (clj->js
             (map (fn [[delay-id kind]]
                    (-> (js/Promise.resolve
                         (update-delayed-event! delay-id "cancel"))
                        (.then
                         (fn [_]
                           (log/debug "Cancelled MatrixRTC delayed leave"
                                      {:kind kind :delay-id delay-id})
                           #js {"kind" (name kind)
                                "delayId" delay-id
                                "ok" true})
                         (fn [err]
                           (log/debug "Could not cancel MatrixRTC delayed leave"
                                      {:kind kind :delay-id delay-id :error err})
                           #js {"kind" (name kind)
                                "delayId" delay-id
                                "ok" false}))))
                  entries))]
        (-> (js/Promise.all promises)
            (.then
             (fn [results]
               (let [failed
                     (reduce
                      (fn [acc result]
                        (if (aget result "ok")
                          acc
                          (assoc acc
                                 (aget result "delayId")
                                 (keyword (aget result "kind")))))
                      {}
                      (array-seq results))]
                 (reset! !delayed-leave-ids failed)
                 (empty? failed)))))))))

(defn- restart-delayed-leaves!
  [generation]
  (when (and (= generation @!widget-generation)
             (seq @!delayed-leave-ids)
             (not @!delayed-leave-heartbeat-in-flight?))
    (reset! !delayed-leave-heartbeat-in-flight? true)
    (let [entries  (seq @!delayed-leave-ids)
          promises
          (clj->js
           (map (fn [[delay-id kind]]
                  (-> (js/Promise.resolve
                       (update-delayed-event! delay-id "restart"))
                      (.then (fn [_] true)
                             (fn [err]
                               (log/warn "MatrixRTC delayed-leave heartbeat failed"
                                         {:kind kind
                                          :delay-id delay-id
                                          :error err})
                               false))))
                entries))]
      (-> (js/Promise.all promises)
          (.then
           (fn [results]
             (reset! !delayed-leave-heartbeat-in-flight? false)
             (when (= generation @!widget-generation)
               (log/debug "MatrixRTC delayed-leave heartbeat"
                          {:delay-ids @!delayed-leave-ids
                           :ok (every? true? (js->clj results))})))
           (fn [err]
             (reset! !delayed-leave-heartbeat-in-flight? false)
             (log/warn "MatrixRTC delayed-leave heartbeat batch failed:" err)))))))

(defn- start-delayed-leave-heartbeat!
  [generation]
  (stop-delayed-leave-heartbeat!)
  (when (seq @!delayed-leave-ids)
    (reset! !delayed-leave-heartbeat
            (js/setInterval
             #(restart-delayed-leaves! generation)
             delayed-leave-heartbeat-ms))))

(defn- schedule-delayed-leaves!
  [generation room-id user-id device-id]
  (go
    (stop-delayed-leave-heartbeat!)
    (try
      (<p! (cancel-delayed-leaves!))
      (catch :default e
        (log/debug "Previous MatrixRTC delayed leave could not be cancelled:" e)))

    (let [state-key-3401 (str "_" user-id "_" device-id "_m.call")
          ids            (atom @!delayed-leave-ids)]
      (try
        (let [response (<p! (delayed-state-event!
                             room-id
                             "org.matrix.msc3401.call.member"
                             state-key-3401))
              delay-id (when response (aget response "delay_id"))]
          (if delay-id
            (swap! ids assoc delay-id :msc3401)
            (log/warn "Rust widget delayed send returned no delay_id"
                      {:event-type "org.matrix.msc3401.call.member"
                       :state-key state-key-3401
                       :response response})))
        (catch :default e
          (log/warn "Could not schedule MatrixRTC delayed leave through Rust widget:" e)))

      (when (= generation @!widget-generation)
        (reset! !delayed-leave-ids @ids)
        (if (seq @ids)
          (do
            (log/info "MatrixRTC delayed leave armed through Rust widget"
                      {:timeout-ms delayed-leave-timeout-ms
                       :heartbeat-ms delayed-leave-heartbeat-ms
                       :delay-ids @ids})
            (start-delayed-leave-heartbeat! generation)
            (restart-delayed-leaves! generation))
          (log/warn "MatrixRTC delayed leave unavailable; continuing without crash cleanup")))

      {:status (if (seq @ids) :success :unavailable)
       :delay-ids @ids})))

(def rtc-notification-event-type "org.matrix.msc4075.rtc.notification")
(def rtc-notification-lifetime-ms 5000)

(defn- joined-room-user-ids
  [room]
  (try
    (->> (array-seq (.getJoinedMembers room))
         (keep (fn [member]
                 (or (.-userId member)
                     (.-user_id member))))
         distinct
         vec)
    (catch :default e
      (log/warn "Failed to enumerate joined room members for RTC notification:" e)
      [])))

(defn- send-rtc-start-notification!
  [room-id room user-id]
  (go
    (try
      (let [recipients        (->> (joined-room-user-ids room)
                                   (remove #(= % user-id))
                                   vec)
            direct-call?      (= 1 (count recipients))
            notification-type (if direct-call? "ring" "notification")]
        (if (empty? recipients)
          (do
            (log/info "Skipping MatrixRTC start notification: no other joined users"
                      {:room-id room-id})
            {:status :skipped :reason :no-recipients})
          (let [content  #js {"notification_type" notification-type
                              "sender_ts" (.getTime (js/Date.))
                              "lifetime" rtc-notification-lifetime-ms
                              "m.call.intent" "video"
                              "m.mentions" #js {"user_ids" (clj->js recipients)}}
                response (<! (dispatch-widget-req!
                              "send_event"
                              #js {"type" rtc-notification-event-type
                                   "content" content}))
                error    (when response (aget response "error"))]
            (if error
              (do
                (log/warn "MatrixRTC start notification rejected"
                          {:room-id room-id
                           :notification-type notification-type
                           :recipients recipients
                           :error error})
                {:status :error :error error})
              (do
                (log/info "MatrixRTC start notification sent"
                          {:room-id room-id
                           :notification-type notification-type
                           :recipients recipients
                           :event-id (when response (aget response "event_id"))})
                {:status :success
                 :notification-type notification-type
                 :event-id (when response (aget response "event_id"))})))))
      (catch :default e
        (log/warn "Failed to send MatrixRTC start notification:" e)
        {:status :error :msg (str e)}))))

(defn- reset-local-key-state!
  []
  (reset! !local-key-index 0)
  (reset! !local-key-material nil)
  (reset! !local-key-creation-ts 0)
  (reset! !shared-with-users #{}))

(defn- initialize-local-key!
  []
  (let [arr (js/Uint8Array. 16)
        now (.getTime (js/Date.))]
    (.getRandomValues js/crypto arr)
    (reset! !local-key-index 0)
    (reset! !local-key-material
            (js/btoa (js/String.fromCharCode.apply nil arr)))
    (reset! !local-key-creation-ts now)
    (reset! !shared-with-users #{})))

(defn broadcast-current-key! [room-id force-target-user]
  (go
    (try
      (let [generation   @!widget-generation
            widget-handle @!widget-handle
            widget-id     @!active-widget-id
            client        @state/!client
            r-id-str      (str room-id)
            room          (.getRoom client r-id-str)]
        (when (and room (.isEncrypted room))
          (let [session      (.session client)
                hs-url       (.-homeserverUrl session)
                access-token (.-accessToken session)
                clean-hs     (str/replace hs-url #"/+$" "")
                user-id      (.-userId session)
                device-id    (or (.-deviceId session) "paradise-web")
                b64-std      @!local-key-material
                current-idx  @!local-key-index
                lk-identity  (str user-id ":" device-id)
                payload      #js {"room_id" r-id-str
                                  "sent_ts" (.getTime (js/Date.))
                                  "session" #js {"application" "m.call" "call_id" "" "scope" "m.room"}
                                  "keys"    #js {"index" current-idx "key" b64-std}
                                  "member"  #js {"claimed_device_id" device-id "id" lk-identity}}]
            (log/info "Broadcasting outbound key index:" current-idx)
            (worker/stream! {:type             "call-keys-update"
                             :room-id          r-id-str
                             :participant-id lk-identity
                             :key-index      current-idx
                             :key-array      b64-std})

            (let [active-users (if (.-activeRoomCallParticipants room)
                                 (js->clj (.activeRoomCallParticipants room))
                                 [])
                  target-users (set active-users)
                  target-users (cond-> target-users force-target-user (conj force-target-user))
                  target-users (disj target-users user-id)
                  query-req    #js {}]
              (doseq [uid target-users]
                (aset query-req uid #js []))
              (when (pos? (count target-users))
                (let [keys-url    (str clean-hs "/_matrix/client/v3/keys/query")
                      keys-resp   (<p! (js/fetch keys-url #js {:method "POST"
                                                               :headers #js {"Authorization" (str "Bearer " access-token)
                                                                             "Content-Type" "application/json"}
                                                               :body (js/JSON.stringify #js {"device_keys" query-req})}))
                      keys-data   (<p! (.json keys-resp))
                      device-keys (aget keys-data "device_keys")
                      messages    #js {}]

                  (when device-keys
                    (doseq [uid (js/Object.keys device-keys)]
                      (when-let [user-devices (aget device-keys uid)]
                        (let [user-msgs #js {}]
                          (doseq [did (js/Object.keys user-devices)]
                            (aset user-msgs did payload))
                          (aset messages uid user-msgs)))))

                  (log/info "Dispatching key index" current-idx "to" (.-length (js/Object.keys messages)) "users via to_device")

                  (if widget-handle
                    (let [msg #js {"api"       "fromWidget"
                                   "widgetId"  widget-id
                                   "requestId" (str "req-" (random-uuid))
                                   "action"    "send_to_device"
                                   "data"      #js {"type" "io.element.call.encryption_keys"
                                                    "encrypted" true
                                                    "messages" messages}}]
                      (when (widget-active? generation widget-handle)
                        (if-let [send-promise
                                 (queue-widget-send! generation widget-handle msg)]
                          (when-not (<p! send-promise)
                            (log/warn "MatrixRTC key to-device send returned false"
                                      {:index current-idx
                                       :targets (count target-users)}))
                          (log/warn "MatrixRTC key to-device send could not be queued"))))
                    (log/warn "Cannot send To-Device payload: Widget handle missing!"))))))))
      {:status :success}
      (catch :default e
        (log/error "Failed to broadcast E2EE keys:" e)
        {:status :error :msg (str e)}))))

(defn rotate-and-broadcast-keys!
  ([room-id] (rotate-and-broadcast-keys! room-id nil false))
  ([room-id force-target-user] (rotate-and-broadcast-keys! room-id force-target-user false))
  ([room-id force-target-user force-rotate?]
   (go
     (try
       (let [client       @state/!client
             r-id-str     (str room-id)
             room         (.getRoom client r-id-str)]
         (when (and room (.isEncrypted room))
           (let [state              (.-currentState room)
                 events-4143        (if state (.getStateEvents state "org.matrix.msc4143.rtc.member") #js [])
                 participant-count  (.-length events-4143)
                 suppress-rotation? (>= participant-count key-rotation-participant-limit)
                 now                (.getTime (js/Date.))
                 key-age            (- now @!local-key-creation-ts)
                 grace-period?      (< key-age key-rotation-grace-period-ms)
                 should-rotate?     (and (not suppress-rotation?)
                                         (or force-rotate? (not grace-period?)))]
             (if should-rotate?
               (let [array   (js/Uint8Array. 16)
                     _       (.getRandomValues js/crypto array)
                     b64-std (js/btoa (js/String.fromCharCode.apply nil array))]
                 (reset! !local-key-material b64-std)
                 (reset! !local-key-creation-ts now)
                 (swap! !local-key-index #(mod (inc %) 256))
                 (log/info "Rotated local key material. New index:" @!local-key-index)
                 (<! (broadcast-current-key! room-id force-target-user)))
               (<! (broadcast-current-key! room-id force-target-user))))))
       {:status :success}
       (catch :default e
         (log/error "Failed to rotate keys:" e)
         {:status :error :msg (str e)})))))

(bind/register! :matrix :call/broadcast-e2ee-key
  (fn [{:keys [room-id]}]
    (broadcast-current-key! room-id nil)))

(bind/register! :matrix :call/rotate-e2ee-keys
  (fn [{:keys [room-id force-rotate?]}]
    (rotate-and-broadcast-keys! room-id nil force-rotate?)))

(def capabilities-provider
  #js {:acquireCapabilities
       (fn [_requested-caps]
         (try
           (let [client    @state/!client
                 user-id   (.userId client)
                 device-id (or (some-> client .session .-deviceId) "paradise-web")]
             (sdk/getElementCallRequiredPermissions user-id device-id))
           (catch :default e
             (log/error "Capability Error:" e)
             (throw e))))})

(defn- widget-response-data
  [action data room-id user-id device-id]
  (cond
    (= action "supported_api_versions")
    #js {"supported_versions" #js ["0.0.1" "0.0.2" "0.2.0"]}

    (= action "capabilities")
    #js {"capabilities"
         #js ["m.always_on_screen"
              "org.matrix.msc4039.download_file"
              (str "org.matrix.msc2762.timeline:" room-id)
              "org.matrix.msc2762.send.event:org.matrix.msc4075.call.notify"
              "org.matrix.msc2762.send.event:org.matrix.msc4075.rtc.notification"
              "org.matrix.msc2762.send.event:org.matrix.rageshake_request"
              "org.matrix.msc2762.send.event:io.element.call.encryption_keys"
              "org.matrix.msc2762.send.event:m.reaction"
              "org.matrix.msc2762.send.event:m.room.redaction"
              "org.matrix.msc2762.send.event:io.element.call.reaction"
              "org.matrix.msc2762.send.event:org.matrix.msc4310.rtc.decline"
              "org.matrix.msc2762.send.event:org.matrix.msc4143.rtc.member"
              "org.matrix.msc2762.receive.event:org.matrix.rageshake_request"
              "org.matrix.msc2762.receive.event:io.element.call.encryption_keys"
              "org.matrix.msc2762.receive.event:m.reaction"
              "org.matrix.msc2762.receive.event:m.room.redaction"
              "org.matrix.msc2762.receive.event:io.element.call.reaction"
              "org.matrix.msc2762.receive.event:org.matrix.msc4310.rtc.decline"
              "org.matrix.msc2762.receive.event:org.matrix.msc4143.rtc.member"
              (str "org.matrix.msc2762.send.state_event:org.matrix.msc3401.call.member#" user-id)
              (str "org.matrix.msc2762.send.state_event:org.matrix.msc3401.call.member#_" user-id "_" device-id "_m.call")
              (str "org.matrix.msc2762.send.state_event:org.matrix.msc3401.call.member#" user-id "_" device-id "_m.call")
              "org.matrix.msc2762.receive.state_event:m.room.create"
              "org.matrix.msc2762.receive.state_event:m.room.name"
              "org.matrix.msc2762.receive.state_event:m.room.member"
              "org.matrix.msc2762.receive.state_event:m.room.encryption"
              "org.matrix.msc2762.receive.state_event:org.matrix.msc3401.call.member"
              "org.matrix.msc3819.send.to_device:m.call.invite"
              "org.matrix.msc3819.send.to_device:m.call.candidates"
              "org.matrix.msc3819.send.to_device:m.call.answer"
              "org.matrix.msc3819.send.to_device:m.call.hangup"
              "org.matrix.msc3819.send.to_device:m.call.reject"
              "org.matrix.msc3819.send.to_device:m.call.select_answer"
              "org.matrix.msc3819.send.to_device:m.call.negotiate"
              "org.matrix.msc3819.send.to_device:m.call.sdp_stream_metadata_changed"
              "org.matrix.msc3819.send.to_device:org.matrix.call.sdp_stream_metadata_changed"
              "org.matrix.msc3819.send.to_device:m.call.replaces"
              "org.matrix.msc3819.send.to_device:io.element.call.encryption_keys"
              "org.matrix.msc3819.receive.to_device:m.call.invite"
              "org.matrix.msc3819.receive.to_device:m.call.candidates"
              "org.matrix.msc3819.receive.to_device:m.call.answer"
              "org.matrix.msc3819.receive.to_device:m.call.hangup"
              "org.matrix.msc3819.receive.to_device:m.call.reject"
              "org.matrix.msc3819.receive.to_device:m.call.select_answer"
              "org.matrix.msc3819.receive.to_device:m.call.negotiate"
              "org.matrix.msc3819.receive.to_device:m.call.sdp_stream_metadata_changed"
              "org.matrix.msc3819.receive.to_device:org.matrix.call.sdp_stream_metadata_changed"
              "org.matrix.msc3819.receive.to_device:m.call.replaces"
              "org.matrix.msc3819.receive.to_device:io.element.call.encryption_keys"
              "org.matrix.msc4157.send.delayed_event"
              "org.matrix.msc4157.update_delayed_event"
              "org.matrix.msc4407.send.sticky_event"
              "org.matrix.msc4407.receive.sticky_event"]}

    (= action "notify_capabilities") #js {}
    (= action "update_state") #js {}

    (= action "openid_credentials")
    (let [orig-req-id (aget data "original_request_id")]
      (when orig-req-id
        (resolve-rpc! orig-req-id data))
      #js {})

    :else #js {}))

(defn- process-widget-message!
  [generation handle room-id user-id device-id widget-id msg-string]
  (try
    (let [msg-obj  (js/JSON.parse msg-string)
          req-id   (aget msg-obj "requestId")
          action   (aget msg-obj "action")
          api      (aget msg-obj "api")
          response (aget msg-obj "response")
          is-req   (and req-id (= api "toWidget") (not response))
          data     (aget msg-obj "data")]

      (log/info "[MatrixRTC widget <- Rust]"
                {:api        api
                 :action     action
                 :request-id req-id
                 :response?  (boolean response)
                 :type       (when data (aget data "type"))
                 :sender     (when data (aget data "sender"))})

      (when (and response (= api "fromWidget"))
        (when-let [cb-map (get @!rpc-callbacks req-id)]
          (when-not (= (:action cb-map) "get_openid")
            (resolve-rpc! req-id response))))

      (when (and (= api "toWidget") data)
        (let [type   (aget data "type")
              sender (aget data "sender")]
          (when (and (or (= type "io.element.call.encryption_keys")
                         (= type "org.matrix.call.encryption_keys"))
                     sender
                     (aget data "content"))
            (let [content (aget data "content")
                  k-val   (aget content "keys")
                  member  (aget content "member")
                  did     (when member (aget member "claimed_device_id"))]
              (when (and k-val did)
                (let [k-arr (if (js/Array.isArray k-val) k-val #js [k-val])
                      lk-id (str sender ":" did)]
                  (doseq [k-obj (array-seq k-arr)]
                    (log/info "[Inbound] Key intercepted for:" lk-id
                              "Idx:" (aget k-obj "index"))
                    (worker/stream! {:type             "call-keys-update"
                                     :room-id          room-id
                                     :participant-id lk-id
                                     :key-index      (aget k-obj "index")
                                     :key-array      (aget k-obj "key")}))))))

          (when (and (= action "send_event")
                     sender
                     (not= sender user-id)
                     (or (= type "org.matrix.msc3401.call.member")
                         (= type "org.matrix.msc4143.rtc.member")))
            (log/info "[MatrixRTC] Sending current E2EE key to member:" sender)
            (broadcast-current-key! room-id sender))))

      (if is-req
        (let [resp-data (widget-response-data action data room-id user-id device-id)
              payload   #js {"api"       "toWidget"
                             "widgetId"  widget-id
                             "requestId" req-id
                             "action"    action
                             "data"      #js {}
                             "response"  resp-data}
              p         (queue-widget-send! generation handle payload)]
          (if p
            (.then p
                   (fn [ok?]
                     (when-not ok?
                       (log/warn "MatrixRTC widget response send returned false"
                                 {:action action :request-id req-id}))
                     ok?))
            (do
              (log/warn "MatrixRTC widget response could not be queued"
                        {:action action :request-id req-id})
              (js/Promise.resolve false))))
        (js/Promise.resolve true)))
    (catch :default e
      (log/error "Worker parse failed:" e)
      (js/Promise.resolve false))))

(defn- start-widget-recv-loop!
  [generation handle room-id user-id device-id widget-id]
  (letfn [(recv-next! []
            (when (widget-active? generation handle)
              (swap! !ffi-recv-started inc)
              (let [p (try
                        (js/Promise.resolve (.recv handle))
                        (catch :default e
                          (when (= generation @!widget-generation)
                            (swap! !ffi-recv-finished inc)
                            (swap! !ffi-recv-failed inc))
                          (when (widget-active? generation handle)
                            (log/error "WASM recv threw:" e))
                          nil))]
                (when p
                  (.then p
                         (fn [msg-string]
                           (when (= generation @!widget-generation)
                             (swap! !ffi-recv-finished inc))
                           (cond
                             (or (nil? msg-string) (= msg-string "null"))
                             (do
                               (when (= generation @!widget-generation)
                                 (swap! !ffi-recv-null inc))
                               (when (widget-active? generation handle)
                                 (log/warn "MatrixRTC widget recv returned nil; driver channel closed")))

                             (widget-active? generation handle)
                             (do
                               (swap! !ffi-recv-messages inc)
                               (-> (process-widget-message!
                                    generation handle room-id user-id device-id widget-id msg-string)
                                   (.then (fn [_]
                                            (when (widget-active? generation handle)
                                              (recv-next!)))
                                          (fn [err]
                                            (log/error "MatrixRTC widget message processing failed:" err)
                                            (when (widget-active? generation handle)
                                              (recv-next!))))))))
                         (fn [err]
                           (when (= generation @!widget-generation)
                             (swap! !ffi-recv-finished inc)
                             (swap! !ffi-recv-failed inc))
                           (when (widget-active? generation handle)
                             (log/error "WASM recv failed:" err))))))))]
    (recv-next!)))

(defn- start-widget-run!
  [generation driver handle room]
  (swap! !ffi-run-started inc)
  (let [run-promise
        (try
          (js/Promise.resolve (.run driver room capabilities-provider))
          (catch :default e
            (when (widget-active? generation handle)
              (stop-widget-runtime!))
            (throw e)))]
    (reset! !widget-run-promise run-promise)
    (.then run-promise
           (fn [_]
             (when (= generation @!widget-generation)
               (swap! !ffi-run-finished inc))
             (when (widget-active? generation handle)
               (log/info "MatrixRTC widget driver run future completed")))
           (fn [err]
             (when (= generation @!widget-generation)
               (swap! !ffi-run-finished inc)
               (swap! !ffi-run-failed inc))
             (when (widget-active? generation handle)
               (log/error "MatrixRTC widget driver run future failed:" err))))))

(defn- bootstrap-widget!
  [generation handle widget-id]
  (js/setTimeout
   (fn []
     (when (widget-active? generation handle)
       (when-let [p (queue-widget-send!
                     generation handle
                     #js {"api"       "fromWidget"
                          "widgetId"  widget-id
                          "requestId" "boot-1"
                          "action"    "supported_api_versions"
                          "data"      #js {"supported_versions" #js ["0.0.1" "0.0.2" "0.2.0"]}})]
         (.then p
                (fn [_]
                  (when (widget-active? generation handle)
                    (js/setTimeout
                     (fn []
                       (when (widget-active? generation handle)
                         (queue-widget-send!
                          generation handle
                          #js {"api"       "fromWidget"
                               "widgetId"  widget-id
                               "requestId" "boot-2"
                               "action"    "content_loaded"
                               "data"      #js {}})))
                     100)))))))
   50))

(bind/register! :matrix :call/start-crypto-driver
  (fn [{:keys [room-id]}]
    (go
      (try
        (if-let [room (.getRoom @state/!client room-id)]
          (do
            (stop-delayed-leave-heartbeat!)
            (stop-widget-runtime!)
            (let [generation    @!widget-generation
                  client        @state/!client
                  session       (.session client)
                  hs-url        (.-homeserverUrl session)
                  access-token  (.-accessToken session)
                  clean-hs      (str/replace hs-url #"/+$" "")
                  user-id       (.-userId session)
                  device-id     (or (.-deviceId session) "paradise-web")
                  widget-id     (str "element-call-" room-id)
                  widget-url    (str "https://call.element.io?widgetId=" widget-id)
                  state-url     (str clean-hs "/_matrix/client/v3/rooms/"
                                     (js/encodeURIComponent room-id)
                                     "/state/im.vector.modular.widgets/"
                                     (js/encodeURIComponent widget-id))
                  widget-body   (js/JSON.stringify
                                 #js {:name "Element Call"
                                      :type "m.custom"
                                      :url widget-url
                                      :creatorUserId user-id
                                      :data #js {}})
                  _             (<p! (js/fetch state-url
                                               #js {:method "PUT"
                                                    :headers #js {"Authorization" (str "Bearer " access-token)
                                                                  "Content-Type" "application/json"}
                                                    :body widget-body}))
                  props         #js {:elementCallUrl "https://call.element.io"
                                     :widgetId widget-id
                                     :encryption (new (.-PerParticipantKeys sdk/EncryptionSystem))}
                  config        #js {:hideHeader true}
                  settings      (sdk/newVirtualElementCallWidget props config)
                  driver-bundle (sdk/makeWidgetDriver settings)
                  driver        (.-driver driver-bundle)
                  handle        (.-handle driver-bundle)]

              (when-not (= generation @!widget-generation)
                (call-first-method! driver ["stop" "close" "dispose" "destroy"])
                (call-first-method! handle ["close" "stop" "dispose" "destroy"])
                (call-method! driver "free")
                (call-method! handle "free")
                (throw (js/Error. "Crypto driver start superseded by a newer start")))

              (reset! !widget-driver driver)
              (reset! !active-widget-id widget-id)
              (reset! !widget-handle handle)
              (reset-ffi-debug-counters!)
              (clear-ffi-send-queue!)

              (start-widget-run! generation driver handle room)
              (start-ffi-debug-logger! generation handle)
              (initialize-local-key!)
              (start-widget-recv-loop! generation handle room-id user-id device-id widget-id)
              (bootstrap-widget! generation handle widget-id)

              {:status :success}))
          {:status :error :msg (str "Room not found: " room-id)})
        (catch :default e
          (log/error "CRASH:" e)
          {:status :error :msg (str e)})))))

(defn- post-json-response!
  [url body]
  (-> (js/fetch
       url
       #js {:method "POST"
            :headers #js {"Content-Type" "application/json"
                          "Accept" "application/json"}
            :body (js/JSON.stringify body)})
      (.then
       (fn [resp]
         (-> (.text resp)
             (.then
              (fn [text]
                (let [data (try
                             (if (seq text)
                               (js/JSON.parse text)
                               #js {})
                             (catch :default _
                               #js {"error" text}))]
                  #js {"response" resp
                       "data" data}))))))))

(defn- usable-sfu-token-response?
  [resp data]
  (let [status (when resp (.-status resp))]
    (boolean
     (and resp
          (number? status)
          (<= 200 status)
          (< status 300)
          (aget data "url")
          (aget data "jwt")))))

(defn- acquire-legacy-sfu-token!
  [legacy-auth-url legacy-body modern-failure]
  (.then
   (post-json-response! legacy-auth-url legacy-body)
   (fn [result]
     (let [resp   (aget result "response")
           data   (or (aget result "data") #js {})
           status (when resp (.-status resp))]
       (if (usable-sfu-token-response? resp data)
         (do
           (log/info
            "MatrixRTC token acquired via legacy /sfu/get"
            {:endpoint legacy-auth-url
             :status status})
           data)
         (throw
          (js/Error.
           (str "SFU legacy token error (HTTP "
                status
                "): "
                (js/JSON.stringify data)
                "; modern /get_token "
                modern-failure))))))
   (fn [err]
     (throw
      (js/Error.
       (str "SFU legacy /sfu/get fetch failed: "
            err
            "; modern /get_token "
            modern-failure))))))

(defn- acquire-sfu-token!
  [modern-auth-url modern-body legacy-auth-url legacy-body]
  (.then
   (post-json-response! modern-auth-url modern-body)
   (fn [result]
     (let [resp    (aget result "response")
           data    (or (aget result "data") #js {})
           status  (when resp (.-status resp))
           errcode (aget data "errcode")]
       (if (usable-sfu-token-response? resp data)
         (do
           (log/info
            "MatrixRTC token acquired via /get_token"
            {:endpoint modern-auth-url
             :status status})
           data)
         (let [modern-failure
               (str "returned HTTP " status " " (js/JSON.stringify data))]
           (log/warn
            "MatrixRTC /get_token failed; falling back to legacy /sfu/get"
            {:endpoint modern-auth-url
             :status status
             :errcode errcode})
           (acquire-legacy-sfu-token!
            legacy-auth-url legacy-body modern-failure)))))
   (fn [err]
     (let [modern-failure (str "was unreachable: " err)]
       (log/warn
        "MatrixRTC /get_token fetch failed; falling back to legacy /sfu/get"
        {:endpoint modern-auth-url
         :error (str err)})
       (acquire-legacy-sfu-token!
        legacy-auth-url legacy-body modern-failure)))))

(bind/register! :matrix :call/request-sfu-token
                (fn [{:keys [room-id]}]
                  (go
                    (try
                      (let [client       @state/!client
                            session      (.session client)
                            hs-url       (.-homeserverUrl session)
                            access-token (.-accessToken session)
                            clean-hs     (str/replace hs-url #"/+$" "")
                            user-id      (.-userId session)
                            device-id    (or (.-deviceId session) "paradise-web")
                            generation   @!widget-generation
                            room         (.getRoom client (str room-id))
                            existing-call-participants
                            (if (and room (.-activeRoomCallParticipants room))
                              (js->clj (.activeRoomCallParticipants room))
                              [])
                            call-was-empty?
                            (zero? (count existing-call-participants))]

                        (when-not room
                          (throw
                           (js/Error.
                            (str "Room not found: " room-id))))

                        (let [wk-url
                              (str hs-url "/.well-known/matrix/client")

                              wk-resp
                              (<p! (js/fetch wk-url))

                              wk-data
                              (<p! (.json wk-resp))

                              foci
                              (or
                               (aget wk-data "org.matrix.msc4143.rtc_foci")
                               (aget wk-data "im.vector.rtc.sfu"))

                              sfu-base-url
                              (when (and foci (pos? (.-length foci)))
                                (.-livekit_service_url (aget foci 0)))]

                          (when-not sfu-base-url
                            (throw
                             (js/Error.
                              "No LiveKit SFU endpoint found")))

                          (let [openid-resp
                                (<! (dispatch-widget-req!
                                     "get_openid"
                                     #js {}))]

                            (when-not (aget openid-resp "access_token")
                              (throw
                               (js/Error.
                                "Widget driver refused OpenID request")))

                            (let [member-id
                                  (str user-id ":" device-id)

                                  slot-id
                                  "m.call#"

                                  auth-base
                                  (str/replace sfu-base-url #"/+$" "")

                                  modern-auth-url
                                  (str auth-base "/get_token")

                                  legacy-auth-url
                                  (str auth-base "/sfu/get")

                                  expires-in
                                  (aget openid-resp "expires_in")

                                  openid-token
                                  #js {"access_token"
                                       (str (aget openid-resp "access_token"))
                                       "token_type"
                                       (or (aget openid-resp "token_type") "Bearer")
                                       "matrix_server_name"
                                       (str (aget openid-resp "matrix_server_name"))}

                                  _
                                  (when (number? expires-in)
                                    (aset openid-token "expires_in" expires-in))

                                  modern-body
                                  #js {"room_id" (str room-id)
                                       "slot_id" slot-id
                                       "openid_token" openid-token
                                       "member"
                                       #js {"id" member-id
                                            "claimed_user_id" user-id
                                            "claimed_device_id" device-id}}

                                  legacy-body
                                  #js {"room" (str room-id)
                                       "openid_token" openid-token
                                       "device_id" device-id}

                                  _
                                  (log/debug
                                   "MatrixRTC /get_token body"
                                   (js/JSON.stringify
                                    #js {"room_id" (str room-id)
                                         "slot_id" slot-id
                                         "openid_token"
                                         #js {"access_token" "<redacted>"
                                              "token_type" (aget openid-token "token_type")
                                              "matrix_server_name" (aget openid-token "matrix_server_name")
                                              "expires_in" (aget openid-token "expires_in")}
                                         "member"
                                         #js {"id" member-id
                                              "claimed_user_id" user-id
                                              "claimed_device_id" device-id}}))

                                  sfu-data
                                  (<p! (acquire-sfu-token!
                                        modern-auth-url
                                        modern-body
                                        legacy-auth-url
                                        legacy-body))]

                                (let [livekit-url
                                      (aget sfu-data "url")

                                      livekit-jwt
                                      (aget sfu-data "jwt")

                                      now
                                      (.getTime (js/Date.))

                                      content-3401
                                      #js {"application"
                                           "m.call"

                                           "call_id"
                                           ""

                                           "device_id"
                                           device-id

                                           "created_ts"
                                           now

                                           "expires"
                                           14400000

                                           "foci_preferred"
                                           #js [#js {"type"
                                                     "livekit"

                                                     "livekit_alias"
                                                     room-id

                                                     "livekit_service_url"
                                                     sfu-base-url}]

                                           "focus_active"
                                           #js {"type"
                                                "livekit"

                                                "focus_selection"
                                                "multi_sfu"}

                                           "m.call.intent"
                                           "video"

                                           "membershipID"
                                           member-id

                                           "scope"
                                           "m.room"}

                                      state-key-3401
                                      (str
                                       "_"
                                       user-id
                                       "_"
                                       device-id
                                       "_m.call")

                                      url-3401
                                      (str
                                       clean-hs
                                       "/_matrix/client/v3/rooms/"
                                       (js/encodeURIComponent room-id)
                                       "/state/org.matrix.msc3401.call.member/"
                                       (js/encodeURIComponent
                                        state-key-3401))

                                      content-4143
                                      #js {"application"
                                           #js {"type"
                                                "m.call"

                                                "m.call.intent"
                                                "video"}

                                           "slot_id"
                                           slot-id

                                           "transports"
                                           #js {"published"
                                                #js [#js {"type"
                                                          "livekit"

                                                          "livekit_service_url"
                                                          sfu-base-url}]

                                                "can_subscribe"
                                                #js ["livekit"]}

                                           "member"
                                           #js {"user_id"
                                                user-id

                                                "device_id"
                                                device-id

                                                "id"
                                                member-id}

                                           "versions"
                                           #js []

                                           "msc4354_sticky_key"
                                           member-id}

                                      url-4143
                                      (str
                                       clean-hs
                                       "/_matrix/client/v3/rooms/"
                                       (js/encodeURIComponent room-id)
                                       "/state/org.matrix.msc4143.rtc.member/"
                                       (js/encodeURIComponent
                                        member-id))

                                      content-slot
                                      #js {"status"
                                           "open"

                                           "application"
                                           #js {"type" "m.call"}

                                           "updated_ts"
                                           now}

                                      url-slot-4143
                                      (str
                                       clean-hs
                                       "/_matrix/client/v3/rooms/"
                                       (js/encodeURIComponent room-id)
                                       "/state/org.matrix.msc4143.rtc.slot/"
                                       (js/encodeURIComponent
                                        slot-id))

                                      url-slot-stbl
                                      (str
                                       clean-hs
                                       "/_matrix/client/v3/rooms/"
                                       (js/encodeURIComponent room-id)
                                       "/state/m.rtc.slot/"
                                       (js/encodeURIComponent
                                        slot-id))

                                      make-put
                                      (fn [body-obj]
                                        #js {:method
                                             "PUT"

                                             :headers
                                             #js {"Authorization"
                                                  (str
                                                   "Bearer "
                                                   access-token)

                                                  "Content-Type"
                                                  "application/json"}

                                             :body
                                             (js/JSON.stringify
                                              body-obj)})]

                                  (when-not
                                   (and livekit-url livekit-jwt)

                                    (throw
                                     (js/Error.
                                      (str
                                       "SFU token response missing url/jwt: "
                                       (js/JSON.stringify
                                        sfu-data)))))

                                  (<!
                                   (schedule-delayed-leaves!
                                    generation
                                    room-id
                                    user-id
                                    device-id))

                                  (let [p3401
                                        (js/fetch
                                         url-3401
                                         (make-put content-3401))

                                        p4143
                                        (js/fetch
                                         url-4143
                                         (make-put content-4143))

                                        pslot1
                                        (js/fetch
                                         url-slot-4143
                                         (make-put content-slot))

                                        pslot2
                                        (js/fetch
                                         url-slot-stbl
                                         (make-put content-slot))]

                                    (<p! p3401)
                                    (<p! p4143)
                                    (<p! pslot1)
                                    (<p! pslot2))

                                  (when call-was-empty?
                                    (<!
                                     (send-rtc-start-notification!
                                      room-id
                                      room
                                      user-id)))

                                  (<!
                                   (broadcast-current-key!
                                    room-id
                                    nil))

                                  {:status :success
                                   :url livekit-url
                                   :token livekit-jwt})))))

                      (catch :default e
                        (stop-delayed-leave-heartbeat!)

                        (log/error
                         "MatrixRTC Handshake Failed:"
                         e)

                        {:status :error
                         :msg (str e)})))))

(bind/register! :matrix :call/leave-sfu
  (fn [{:keys [room-id]}]
    (go
      (let [generation @!widget-generation
            handle     @!widget-handle
            widget-id  @!active-widget-id]
        (stop-delayed-leave-heartbeat!)
        (try
          (let [client       @state/!client
                session      (.session client)
                hs-url       (.-homeserverUrl session)
                access-token (.-accessToken session)
                clean-hs     (str/replace hs-url #"/+$" "")
                user-id      (.-userId session)
                device-id    (or (.-deviceId session) "paradise-web")
                member-id    (str user-id ":" device-id)

                state-key-3401 (str "_" user-id "_" device-id "_m.call")
                url-3401       (str clean-hs "/_matrix/client/v3/rooms/" (js/encodeURIComponent room-id) "/state/org.matrix.msc3401.call.member/" (js/encodeURIComponent state-key-3401))
                url-4143       (str clean-hs "/_matrix/client/v3/rooms/" (js/encodeURIComponent room-id) "/state/org.matrix.msc4143.rtc.member/" (js/encodeURIComponent member-id))
                slot-key       "m.call#"
                url-slot-4143  (str clean-hs "/_matrix/client/v3/rooms/" (js/encodeURIComponent room-id) "/state/org.matrix.msc4143.rtc.slot/" (js/encodeURIComponent slot-key))
                url-slot-stbl  (str clean-hs "/_matrix/client/v3/rooms/" (js/encodeURIComponent room-id) "/state/m.rtc.slot/" (js/encodeURIComponent slot-key))

                make-put       (fn [body-obj]
                                 #js {:method "PUT"
                                      :headers #js {"Authorization" (str "Bearer " access-token)
                                                    "Content-Type" "application/json"}
                                      :body (if (string? body-obj)
                                              body-obj
                                              (js/JSON.stringify body-obj))})]

            (let [p3401    (js/fetch url-3401 (make-put "{}"))
                  p4143    (js/fetch url-4143 (make-put "{}"))
                  slot-body #js {"status" "open"
                                 "application" #js {"type" "m.call"}
                                 "updated_ts" (.getTime (js/Date.))}
                  pslot1   (js/fetch url-slot-4143 (make-put slot-body))
                  pslot2   (js/fetch url-slot-stbl (make-put slot-body))]
              (<p! p3401)
              (<p! p4143)
              (<p! pslot1)
              (<p! pslot2))

            (<p! (cancel-delayed-leaves!))

            (when (= generation @!widget-generation)
              (let [array (js/Uint8Array. 16)
                    now   (.getTime (js/Date.))]
                (.getRandomValues js/crypto array)
                (reset! !local-key-material
                        (js/btoa (js/String.fromCharCode.apply nil array)))
                (reset! !local-key-creation-ts now)
                (swap! !local-key-index #(mod (inc %) 256)))
              (<! (broadcast-current-key! room-id nil)))

            (when (and handle (widget-active? generation handle))
              (when-let [send-aos
                         (queue-widget-send!
                          generation handle
                          #js {"api"       "fromWidget"
                               "widgetId"  widget-id
                               "requestId" (str "req-aos-" (random-uuid))
                               "action"    "set_always_on_screen"
                               "data"      #js {"value" false}})]
                (<p! send-aos))
              (when-let [send-close
                         (queue-widget-send!
                          generation handle
                          #js {"api"       "fromWidget"
                               "widgetId"  widget-id
                               "requestId" (str "req-close-" (random-uuid))
                               "action"    "io.element.close"
                               "data"      #js {}})]
                (<p! send-close)))

            {:status :success})

          (catch :default e
            (log/error "MatrixRTC leave failed:" e)
            {:status :error :msg (str e)})

          (finally
            (when (= generation @!widget-generation)
              (stop-widget-runtime!)
              (reset-local-key-state!))))))))

(bind/register! :matrix :teardown-widget
  (fn [_]
    (stop-delayed-leave-heartbeat!)
    (stop-widget-runtime!)
    (reset-local-key-state!)
    {:status :success}))
