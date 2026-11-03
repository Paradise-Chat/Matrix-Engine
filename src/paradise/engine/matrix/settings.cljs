(ns paradise.engine.matrix.settings
  (:require
   [cljs-workers.worker :as worker]
   [paradise.shared.client.session-store :refer [SessionStore]]
   [paradise.engine.state :as state]
   [paradise.engine.binding :as bind]
   [net :refer [set-auth-context!] :as net]
   [promesa.core :as p]
   [taoensso.timbre :as log]
   [clojure.string :as str]
   ["ffi-bindings" :as sdk]
   [cljs.core.async.interop :refer-macros [<p!]]
   [cljs.core.async :refer [go]]))


(bind/register! :matrix :get-available-accounts
                (fn [_]
                  (go
                    (try
                      (<p! (p/let [store    (SessionStore. :matrix)
                                   sessions (.loadSessions store)]
                             (let [uids (if sessions (js/Object.keys sessions) #js [])
                                   accs (keep (fn [uid]
                                                (when-let [data (aget sessions uid)]
                                                  (when-let [session (.-session data)]
                                                    (let [url (.-homeserverUrl session)]
                                                      {:userId   uid
                                                       :user-id  uid
                                                       :base-url url
                                                       :hs_url   url}))))
                                              uids)]
                               {:status :success :accounts (vec accs)})))
                      (catch :default e
                        {:status :error :msg (str e)})))))

(bind/register! :matrix :get-account-data
                 (fn [{:keys [event-type]}]
                   (go
                     (try
                       (if-let [client @state/!client]
                         (let [data (<p! (.accountData client event-type))]
                           {:status :success :data data})
                         {:status :error :msg "No active client"})
                       (catch :default e
                         {:status :error :msg (str e)})))))

(bind/register! :matrix :recover-session
                 (fn [{:keys [recovery-key]}]
                   (go
                     (try
                       (if-let [client @state/!client]
                         (let [encryption-module (.encryption client)]
                           (log/info "Attempting session recovery with key length:" (count recovery-key))
                           (<p! (.recover encryption-module recovery-key))
                           (log/info "Session recovery successful!")
                           {:status :success})
                         {:status :error :msg "No active client"})
                       (catch :default e
                         (let [err-str (or (.-message e) (str e))]
                           (log/error "Recovery FFI Panic:" err-str)
                           {:status :error :msg err-str}))))))

(defonce !recovery-listener-handle (atom nil))

(defn setup-encryption-listeners! [client]
  (let [encryption-module (.encryption client)
        listener-obj #js {:onUpdate
                          (fn [state-enum]
                            (let [state-kw (cond
                                             (or (= state-enum 1) (= state-enum "Enabled"))    :enabled
                                             (or (= state-enum 2) (= state-enum "Disabled"))   :disabled
                                             (or (= state-enum 3) (= state-enum "Incomplete")) :incomplete
                                             :else :unknown)]
                              (log/info "Worker stream: Recovery state changed to:" state-enum "->" state-kw)
                              (worker/stream! {:type "recovery-state-update"
                                               :state state-kw})))}
        handle (.recoveryStateListener encryption-module listener-obj)]
    (reset! !recovery-listener-handle handle)))



(def ^:private b58-alphabet "123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz")

(defn- decode-recovery-key [recovery-key]
  (let [clean (str/replace (str recovery-key) #"\s+" "")
        bytes (loop [chars (seq clean)
                     acc   (vec (repeat 35 0))]
                (if-let [c (first chars)]
                  (let [val (.indexOf b58-alphabet (str c))]
                    (recur (rest chars)
                           (loop [i     (dec (count acc))
                                  carry val
                                  out   acc]
                             (if (neg? i)
                               out
                               (let [total (+ (* (nth out i) 58) carry)]
                                 (recur (dec i)
                                        (quot total 256)
                                        (assoc out i (mod total 256))))))))
                  acc))]
    (js/Uint8Array. (subvec bytes 2 34))))

(defn- b64->uint8 [b64]
  (let [padded (str b64 (subs "====" (mod (count b64) 4)))
        raw    (js/atob (-> padded (str/replace "-" "+") (str/replace "_" "/")))
        arr    (js/Uint8Array. (.-length raw))]
    (dotimes [i (.-length raw)]
      (aset arr i (.charCodeAt raw i)))
    arr))

(defn- decrypt-ssss-secret [recovery-key secret-name encrypted-entry]
  (p/let [raw-key  (decode-recovery-key recovery-key)
          base-key (.importKey js/crypto.subtle "raw" raw-key "HKDF" false #js ["deriveBits"])
          bits     (.deriveBits js/crypto.subtle
                                #js {:name "HKDF"
                                     :hash "SHA-256"
                                     :salt (js/Uint8Array. 32)
                                     :info (.encode (js/TextEncoder.) secret-name)}
                                base-key
                                512)
          aes-raw  (.slice bits 0 32)
          aes-key  (.importKey js/crypto.subtle "raw" aes-raw #js {:name "AES-CTR"} false #js ["decrypt"])
          iv       (b64->uint8 (:iv encrypted-entry))
          cipher   (b64->uint8 (:ciphertext encrypted-entry))
          plain    (.decrypt js/crypto.subtle
                             #js {:name    "AES-CTR"
                                  :counter iv
                                  :length  64}
                             aes-key
                             cipher)]
    (.decode (js/TextDecoder.) plain)))

(defn- fetch-ssk-seed [base-url token user-id recovery-key]
  (let [clean-url (str/replace (str base-url) #"/+$" "")
        url       (str clean-url "/_matrix/client/v3/user/"
                       (js/encodeURIComponent user-id)
                       "/account_data/m.cross_signing.self_signing")]
    (-> (p/let [resp (net/fetch url #js {:headers #js {"Authorization" (str "Bearer " token)
                                                       "Accept"        "application/json"}})
                json (when (.-ok resp) (.json resp))
                data (when json (js->clj json :keywordize-keys true))
                enc  (some-> data :encrypted vals first)]
          (when enc
            (decrypt-ssss-secret recovery-key "m.cross_signing.self_signing" enc)))
        (p/catch (fn [_] nil)))))

(bind/register! :matrix :prepare-push-verification
  (fn [{:keys [user-id recovery-key]}]
    (go
      (try
        (if-let [client @state/!client]
          (let [session    (.session client)
                base-url   (.-homeserverUrl session)
                main-token (.-accessToken session)
                ssk-seed   (when (and base-url main-token recovery-key)
                             (<p! (fetch-ssk-seed base-url main-token user-id recovery-key)))]
            {:status            :success
             :verification-data {:sskSeed         (or ssk-seed "")
                                 :mainAccessToken (or main-token "")}})
          {:status :error :msg "No active client"})
        (catch :default e
          {:status :error :msg (str e)})))))


(bind/register! :matrix :register-pusher
  (fn [{:keys [p256dh auth endpoint app-id push-url app-name lang pushkey is-native platform]}]
    (go
      (try
        (let [client     @state/!client
              hs-url     (.homeserver client)
              token      (.-accessToken (.session client))
              clean-base (str/replace hs-url #"/+$" "")
              url        (str clean-base "/_matrix/client/v3/pushers/set")
              payload    (cond
                           (and is-native (= platform "ios"))
                           {:kind "http"
                            :app_id app-id
                            :pushkey pushkey
                            :app_display_name app-name
                            :device_display_name "iOS Device"
                            :lang lang
                            :append false
                            :data {:url push-url
                                   :format "event_id_only"
                                   :default_payload {:aps {:alert {:loc-key "New Message"}
                                                           :sound "default"
                                                           :badge 1
                                                           :content-available 1}}}}
                           (and is-native (= platform "android"))
                           {:kind "http"
                            :app_id app-id
                            :pushkey pushkey
                            :app_display_name app-name
                            :device_display_name "Android Device"
                            :lang lang
                            :append false
                            :data {:url push-url
                                   :format "event_id_only"
                                   }}
                           :else
                           {:kind "http"
                            :app_id app-id
                            :pushkey p256dh
                            :app_display_name app-name
                            :device_display_name "Web Browser"
                            :lang lang
                            :append false
                            :data {:url push-url
                                   :events_only true
                                   :endpoint endpoint
                                   :p256dh p256dh
                                   :auth auth}})
              resp       (<p! (net/fetch url #js {:method "POST"
                                                  :headers #js {"Authorization" (str "Bearer " token)
                                                                "Content-Type" "application/json"}
                                                  :body (js/JSON.stringify (clj->js payload))}))]
          (if (.-ok resp)
            {:status "success"}
            (let [err-body (<p! (.json resp))]
              {:status "error" :msg err-body})))
        (catch :default e
          {:status "error" :msg (str e)})))))

(bind/register! :matrix :remove-pusher
  (fn [{:keys [pushkey app-id]}]
    (go
      (try
        (let [client      @state/!client
              identifiers #js {:pushkey pushkey :appId app-id}]
          (<p! (.deletePusher client identifiers))
          {:status "success"})
        (catch :default e
          {:status "error" :msg (str e)})))))


(bind/register! :matrix :clear-all-pushers
  (fn [_]
    (go
      (try
        (let [client     @state/!client
              hs-url     (.homeserver client)
              token      (.-accessToken (.session client))
              clean-base (str/replace hs-url #"/+$" "")
              url        (str clean-base "/_matrix/client/v3/pushers")
              resp       (<p! (net/fetch url #js {:method "GET"
                                                  :headers #js {"Authorization" (str "Bearer " token)}}))]
          (if-not (.-ok resp)
            (let [err-body (<p! (.json resp))]
              {:status "error" :msg err-body})
            (let [data    (<p! (.json resp))
                  pushers (:pushers (js->clj data :keywordize-keys true))]
              (doseq [p pushers]
                (let [pushkey (:pushkey p)
                      app-id  (:app_id p)
                      identifiers #js {:pushkey pushkey :appId app-id}]
                  (try
                    (<p! (.deletePusher client identifiers))
                    (catch :default e
                      (log/warn "Failed to delete pusher" pushkey ":" e)))))
              {:status "success"})))
        (catch :default e
          {:status "error" :msg (str e)})))))



