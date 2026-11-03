(ns paradise.engine.matrix.push-worker
  (:require [clojure.string :as str]))

(defn- crypto-api []
  js/globalThis.ParadisePushCrypto)

(defn- storage-get [k]
  (when (fn? js/globalThis.__hostStorageGet)
    (js/globalThis.__hostStorageGet k)))

(defn- storage-set! [k v]
  (when (and (fn? js/globalThis.__hostStorageSet) (some? v))
    (js/globalThis.__hostStorageSet k v)))

(defn- http-request [method url token body-obj]
  (when (fn? js/globalThis.__hostHttpSync)
    (let [body    (cond
                    (nil? body-obj)    ""
                    (string? body-obj) body-obj
                    :else              (js/JSON.stringify (clj->js body-obj)))
          headers (cond-> {"Accept" "application/json"}
                    token                (assoc "Authorization" (str "Bearer " token))
                    (not (str/blank? body)) (assoc "Content-Type" "application/json"))
          resp    (js/globalThis.__hostHttpSync method url (clj->js headers) body)]
      (when resp
        {:ok     (boolean (aget resp "ok"))
         :status (aget resp "status")
         :body   (try
                   (js->clj (js/JSON.parse (aget resp "body")) :keywordize-keys true)
                   (catch :default _ nil))
         :raw    (aget resp "body")}))))

(defn- http-request-json [method url token body-obj]
  (let [res (http-request method url token body-obj)]
    (when (:ok res)
      (:body res))))


(defn- clean-base-url [u]
  (when (and u (not (str/blank? u)))
    (let [with-proto (if (str/starts-with? u "http") u (str "https://" u))]
      (str/replace with-proto #"/+$" ""))))

(defn- safe-filename [s]
  (str/replace (str s) #"[^a-zA-Z0-9._-]" "_"))

(defn- megolm-storage-key [room-id session-id]
  (str "megolm/" (safe-filename room-id) "_" (safe-filename session-id) ".json"))

(defn- olm-sessions-storage-key [sender-key]
  (str "olm_sessions/" (safe-filename sender-key) ".json"))

(defn- load-olm-sessions [sender-key]
  (if-let [raw (storage-get (olm-sessions-storage-key sender-key))]
    (try
      (vec (js->clj (js/JSON.parse raw)))
      (catch :default _ []))
    []))

(defn- save-olm-sessions! [sender-key sessions]
  (storage-set! (olm-sessions-storage-key sender-key)
                (js/JSON.stringify (clj->js (vec sessions)))))

(defn- try-existing-olm-sessions [api sessions msg-type body]
  (loop [idx 0]
    (when (< idx (count sessions))
      (let [pickle (nth sessions idx)
            res    (try
                     (js->clj (js/JSON.parse (.decrypt_olm_with_session api pickle msg-type body))
                              :keywordize-keys true)
                     (catch :default _ nil))]
        (if res
          {:plaintext (:plaintext res)
           :sessions  (assoc sessions idx (:session_pickle res))}
          (recur (inc idx)))))))

(defn- decrypt-olm-event [api acct-pickle my-curve-key ev]
  (let [content    (:content ev)
        sender-key (:sender_key content)
        cipher-map (:ciphertext content)
        msg-obj    (or (get cipher-map my-curve-key)
                       (get cipher-map (keyword my-curve-key)))
        msg-type   (:type msg-obj)
        body       (:body msg-obj)]
    (if-not (and sender-key msg-obj (number? msg-type) (string? body))
      {:account-pickle acct-pickle :plaintext nil}
      (let [sessions     (load-olm-sessions sender-key)
            existing-res (try-existing-olm-sessions api sessions msg-type body)]
        (if existing-res
          (do
            (save-olm-sessions! sender-key (:sessions existing-res))
            {:account-pickle acct-pickle
             :plaintext      (:plaintext existing-res)})
          (if (= msg-type 0)
            (if-let [prekey-res (try
                                  (js->clj (js/JSON.parse
                                            (.decrypt_olm_prekey_with_account api acct-pickle sender-key body))
                                           :keywordize-keys true)
                                  (catch :default _ nil))]
              (do
                (save-olm-sessions! sender-key (conj sessions (:session_pickle prekey-res)))
                {:account-pickle (:account_pickle prekey-res)
                 :plaintext      (:plaintext prekey-res)})
              {:account-pickle acct-pickle :plaintext nil})
            {:account-pickle acct-pickle :plaintext nil}))))))

(defn- store-decrypted-room-key! [api plaintext]
  (when plaintext
    (when-let [inner (try
                       (js->clj (js/JSON.parse plaintext) :keywordize-keys true)
                       (catch :default _ nil))]
      (let [ev-type     (:type inner)
            content     (:content inner)
            room-id     (:room_id content)
            session-id  (:session_id content)
            session-key (:session_key content)]
        (when (and room-id session-id session-key)
          (cond
            (= ev-type "m.room_key")
            (when-let [megolm-pickle (try
                                       (.create_inbound_group_session api session-key)
                                       (catch :default _ nil))]
              (storage-set! (megolm-storage-key room-id session-id) megolm-pickle)
              true)

            (= ev-type "m.forwarded_room_key")
            (when-let [megolm-pickle (try
                                       (.import_exported_group_session api session-key)
                                       (catch :default _ nil))]
              (storage-set! (megolm-storage-key room-id session-id) megolm-pickle)
              true)

            :else false))))))

(defn- maybe-replenish-otks! [api acct-pickle base-url token user-id device-id otk-count]
  (if (and user-id device-id (number? otk-count) (< otk-count 20))
    (if-let [rep (try
                   (js->clj (js/JSON.parse
                             (.replenish_one_time_keys api acct-pickle user-id device-id 30))
                            :keywordize-keys false)
                   (catch :default _ nil))]
      (let [upload-url (str base-url "/_matrix/client/v3/keys/upload")
            otks       (get rep "one_time_keys")]
        (if (http-request-json "POST" upload-url token {"one_time_keys" otks})
          (get rep "account_pickle")
          acct-pickle))
      acct-pickle)
    acct-pickle))

(defn- sync-to-device-keys! [base-url token session]
  (let [api          (crypto-api)
        acct-initial (storage-get "olm_account.pickle")
        my-curve-key (or (:curve25519_key session) (storage-get "curve25519_key"))
        user-id      (:user_id session)
        device-id    (:device_id session)
        since        (storage-get "sync_batch")
        filter-str   (js/encodeURIComponent
                      "{\"room\":{\"rooms\":[]},\"presence\":{\"types\":[]},\"account_data\":{\"types\":[]}}")
        sync-url     (str base-url "/_matrix/client/v3/sync?timeout=0&filter=" filter-str
                          (when-not (str/blank? since)
                            (str "&since=" (js/encodeURIComponent since))))]
    (if-not (and api acct-initial my-curve-key)
      {:ok false :reason "missing-local-account"}
      (let [resp (http-request "GET" sync-url token nil)]
        (if-not (:ok resp)
          {:ok false :reason (str "sync-http-" (:status resp))}
          (let [sync-res   (:body resp)
                events     (get-in sync-res [:to_device :events])
                next-batch (:next_batch sync-res)
                otk-count  (get-in sync-res [:device_one_time_keys_count :signed_curve25519])
                keys-saved (atom 0)
                acct-after (reduce
                            (fn [curr-acct ev]
                              (if (and (= (:type ev) "m.room.encrypted")
                                       (= (get-in ev [:content :algorithm]) "m.olm.v1.curve25519-aes-sha2"))
                                (let [{:keys [account-pickle plaintext]}
                                      (decrypt-olm-event api curr-acct my-curve-key ev)]
                                  (when (store-decrypted-room-key! api plaintext)
                                    (swap! keys-saved inc))
                                  account-pickle)
                                curr-acct))
                            acct-initial
                            events)
                acct-final (maybe-replenish-otks! api acct-after base-url token user-id device-id otk-count)]
            (storage-set! "olm_account.pickle" acct-final)
            (when next-batch
              (storage-set! "sync_batch" next-batch))
            {:ok         true
             :to-device  (count events)
             :keys-saved @keys-saved}))))))

(defn- try-megolm-decrypt [api store-key ciphertext]
  (when-let [pickle (storage-get store-key)]
    (when-let [res (try
                     (js->clj (js/JSON.parse (.decrypt_megolm api pickle ciphertext))
                              :keywordize-keys true)
                     (catch :default _ nil))]
      (storage-set! store-key (:session_pickle res))
      (try
        (js->clj (js/JSON.parse (:plaintext res)) :keywordize-keys true)
        (catch :default _ nil)))))

(defn- decrypt-megolm-event [raw-event base-url token session]
  (let [api        (crypto-api)
        room-id    (:room_id raw-event)
        content    (:content raw-event)
        session-id (:session_id content)
        ciphertext (:ciphertext content)
        store-key  (megolm-storage-key room-id session-id)]
    (if-not (and api room-id session-id ciphertext)
      {:event nil :diag "missing-megolm-fields"}
      (if-let [cached (try-megolm-decrypt api store-key ciphertext)]
        {:event cached :diag nil}
        (let [sync-stats (sync-to-device-keys! base-url token session)
              synced     (try-megolm-decrypt api store-key ciphertext)]
          (if synced
            {:event synced :diag nil}
            {:event nil
             :diag  (str "no-key(sid=" (subs session-id 0 (min 8 (count session-id)))
                         " to_dev=" (:to-device sync-stats 0)
                         " saved=" (:keys-saved sync-stats 0)
                         (when-let [r (:reason sync-stats)] (str " err=" r))
                         ")")}))))))

(defn- format-matrix-event [event sender-name]
  (let [ev-type (:type event)
        content (:content event)
        msgtype (:msgtype content)]
    (cond
      (= ev-type "m.call.invite") "Incoming call"
      (= ev-type "m.sticker") "Sent a sticker"
      (= ev-type "m.reaction") "Reacted to a message"
      (= ev-type "m.room.member") "Invited you to the room"
      (= ev-type "m.room.message")
      (case msgtype
        ("m.text" "m.notice") (:body content)
        "m.emote" (str "* " sender-name " " (:body content))
        "m.image" "Sent an image"
        "m.video" "Sent a video"
        "m.audio" "Sent an audio clip"
        "m.file"  "Sent a file"
        "Sent a message")
      :else "New Message")))


;; Not sure we want to do this
;; This would make every notif trigger a fetch...
;; And that seems like a bad idea.
;; But the initial event fetch doesn't include
;; the room name so this might be desirable
;; for some?
(defn- fetch-room-name [base-url token room-id]
  (when (and base-url token room-id)
    (let [url (str base-url
                   "/_matrix/client/v3/rooms/"
                   (js/encodeURIComponent room-id)
                   "/state/m.room.name/")
          res (http-request "GET" url token nil)]
      (when (:ok res)
        (let [name-str (get-in res [:body :name])]
          (when (and name-str (not (str/blank? name-str)))
            name-str))))))


(defn ^:export createShadowSession [raw-args]
  (let [args       (js->clj raw-args :keywordize-keys true)
        base-url   (clean-base-url (or (:homeserver args)
                                       (:base-url args)
                                       (:baseUrl args)
                                       (:homeserverUrl args)))
        username   (:username args)
        password   (:password args)
        ssk-seed   (:sskSeed args)
        has-ssk?   (and ssk-seed (not (str/blank? ssk-seed)))
        login-url  (str base-url "/_matrix/client/v3/login")
        login-body {:type                        "m.login.password"
                    :identifier                  {:type "m.id.user" :user username}
                    :password                    password
                    :initial_device_display_name "Paradise Background Sync"}
        login-res  (http-request-json "POST" login-url nil login-body)]
    (if-not (and login-res (:access_token login-res) (:device_id login-res))
      #js {:ok false :error "Matrix shadow login failed"}
      (let [token      (:access_token login-res)
            user-id    (or (:user_id login-res) (:userId args))
            device-id  (:device_id login-res)
            api        (crypto-api)
            prov-str   (.create_shadow_account api user-id device-id 50 ssk-seed)
            prov       (js->clj (js/JSON.parse prov-str) :keywordize-keys false)
            acct-pkl   (get prov "account_pickle")
            curve-key  (get prov "curve25519_key")
            ed-key     (get prov "ed25519_key")
            upload-obj (get prov "upload_payload")
            upload-url (str base-url "/_matrix/client/v3/keys/upload")
            upload-res (http-request "POST" upload-url token upload-obj)]
        (if-not (:ok upload-res)
          #js {:ok false :error (str "Key upload failed: " (:raw upload-res))}
          (do
            (storage-set! "olm_account.pickle" acct-pkl)
            (storage-set! "curve25519_key" curve-key)
            (storage-set! "ed25519_key" ed-key)
            (storage-set! "sync_batch" "")
            (sync-to-device-keys! base-url token {:user_id        user-id
                                                  :device_id      device-id
                                                  :curve25519_key curve-key})
            #js {:ok      true
                 :userId  user-id
                 :session #js {:access_token        token
                               :user_id             user-id
                               :device_id           device-id
                               :homeserver_url      base-url
                               :curve25519_key      curve-key
                               :ed25519_key         ed-key
                               :is_verified         (if has-ssk? "true" "false")
                               :notification_script "push-matrix.js"}}))))))

(defn ^:export activatePushSession [raw-args raw-session]
  (let [args        (js->clj raw-args :keywordize-keys true)
        session     (js->clj raw-session :keywordize-keys true)
        push-token  (:pushToken args)
        push-url    (:pushUrl args)
        app-id      (or (:appId args) "moi.paradise.ios")
        is-android? (or (= (:platform args) "android")
                        (str/includes? (str/lower-case app-id) "android"))
        base-url    (clean-base-url (or (:homeserver_url session) (:base_url session)))
        token       (or (:access_token session) (:token session))
        url         (str base-url "/_matrix/client/v3/pushers/set")
        pusher-data (cond-> {:url    push-url
                             :format "event_id_only"}
                      (not is-android?)
                      (assoc :default_payload {:aps {:mutable-content 1
                                                     :alert           {:loc-key  "Notification"
                                                                       :loc-args []}}}))
        body        {:pushkey             push-token
                     :kind                "http"
                     :app_id              app-id
                     :app_display_name    "Paradise"
                     :device_display_name (if is-android? "Android Device" "iOS Device")
                     :profile_tag         "primary"
                     :lang                "en"
                     :data                pusher-data}
        res         (http-request "POST" url token body)]
    (if (:ok res)
      #js {:ok true}
      #js {:ok false :error (or (:raw res) "Failed to set pusher")})))

(defn ^:export deactivatePushSession [raw-args raw-session]
  (let [args       (js->clj raw-args :keywordize-keys true)
        session    (js->clj raw-session :keywordize-keys true)
        push-token (:pushToken args)
        app-id     (:appId args)
        base-url   (clean-base-url (or (:homeserver_url session) (:base_url session)))
        token      (or (:access_token session) (:token session))
        url        (str base-url "/_matrix/client/v3/pushers/set")
        body       {:pushkey push-token
                    :kind    nil
                    :app_id  app-id}
        res        (http-request "POST" url token body)]
    (if (:ok res)
      #js {:ok true}
      #js {:ok false :error (or (:raw res) "Failed to delete pusher")})))


(defn ^:export importRoomKeys [exported-keys-json]
  (when (and exported-keys-json (not (str/blank? exported-keys-json)))
    (try
      (let [api     (crypto-api)
            entries (js->clj (js/JSON.parse exported-keys-json) :keywordize-keys true)]
        (doseq [{:keys [room_id session_id session_key]} entries]
          (when (and room_id session_id session_key)
            (when-let [pickle (try
                                (.import_exported_group_session api session_key)
                                (catch :default _ nil))]
              (storage-set! (megolm-storage-key room_id session_id) pickle))))
        true)
      (catch :default _ false))))

(defn ^:export verifyPushSession [raw-args raw-session]
  (let [args        (js->clj raw-args :keywordize-keys true)
        session     (js->clj raw-session :keywordize-keys true)
        ssk-seed    (:sskSeed args)
        main-token  (or (:mainAccessToken args) (:access_token session))
        user-id     (:user_id session)
        device-id   (:device_id session)
        base-url    (clean-base-url (:homeserver_url session))
        acct-pickle (storage-get "olm_account.pickle")
        api         (crypto-api)]
    (importRoomKeys (:exportedRoomKeysJson args))
    (if (and ssk-seed (not (str/blank? ssk-seed)) acct-pickle (fn? (.-sign_shadow_device_with_ssk api)))
      (let [sig-str (.sign_shadow_device_with_ssk api acct-pickle user-id device-id ssk-seed)
            sig-obj (js->clj (js/JSON.parse sig-str) :keywordize-keys false)
            url     (str base-url "/_matrix/client/v3/keys/signatures/upload")
            res     (http-request "POST" url main-token sig-obj)]
        (if (:ok res)
          #js {:ok true}
          #js {:ok false :error (or (:raw res) "Failed to upload SSK signature")}))
      #js {:ok true})))


(defn ^:export resolvePushNotification [raw-payload raw-session]
  (let [payload   (js->clj raw-payload :keywordize-keys true)
        session   (js->clj raw-session :keywordize-keys true)
        room-id   (or (:room_id payload)
                      (get-in payload [:data :room_id])
                      (get-in payload [:notification :room_id]))
        event-id  (or (:event_id payload)
                      (get-in payload [:data :event_id])
                      (get-in payload [:notification :event_id]))
        base-url  (clean-base-url (or (:base_url session) (:homeserver_url session)))
        token     (or (:token session) (:access_token session))
        room-name (:room_name payload)]
    (if (and room-id event-id base-url token)
      (let [event-url   (str base-url "/_matrix/client/v3/rooms/"
                             (js/encodeURIComponent room-id)
                             "/event/"
                             (js/encodeURIComponent event-id))
            ev-resp     (http-request "GET" event-url token nil)
            raw-event   (:body ev-resp)
            sender-name (or (:sender_display_name payload)
                            (:sender raw-event)
                            "New Message")]
        (cond
          (not (:ok ev-resp))
          #js {:tag   room-id
               :title sender-name
               :body  (str "Open Paradise to view. [event-http=" (:status ev-resp) "]")}

          (= (:type raw-event) "m.room.encrypted")
          (let [{:keys [event diag]} (decrypt-megolm-event raw-event base-url token session)]
            #js {:tag      room-id
                 :title    sender-name
                 :subtitle room-name
                 :body     (if event
                             (format-matrix-event event sender-name)
                             (str "Open Paradise to view. [" diag "]"))
                 :is_call  (= (:type event) "m.call.invite")
                 :unread   (:unread payload)})

          :else
          #js {:tag      room-id
               :title    sender-name
               :subtitle room-name
               :body     (format-matrix-event raw-event sender-name)
               :is_call  (= (:type raw-event) "m.call.invite")
               :unread   (:unread payload)}))
      #js {:title (or (:sender_display_name payload) "New Message")
           :body  "Open Paradise to view."})))


(set! (.-createShadowSession js/globalThis) createShadowSession)
(set! (.-activatePushSession js/globalThis) activatePushSession)
(set! (.-deactivatePushSession js/globalThis) deactivatePushSession)
(set! (.-verifyPushSession js/globalThis) verifyPushSession)
(set! (.-importRoomKeys js/globalThis) importRoomKeys)
(set! (.-resolvePushNotification js/globalThis) resolvePushNotification)
