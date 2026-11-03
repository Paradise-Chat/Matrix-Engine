(ns paradise.engine.matrix.core
  (:require
   [paradise.engine.binding :as bind]
   [cljs-workers.mesh :as mesh]
   [clojure.string :as str]
   [promesa.core :as p]
   [taoensso.timbre :as log]
   [paradise.shared.client.session-store :as session-store :refer [SessionStore]]
   ["ffi-bindings" :as sdk]
   [cljs.core.async.interop :refer-macros [<p!]]
   [cljs.core.async :refer [go <!]]
   [net :refer [set-auth-context!] :as net]
   [paradise.engine.state :as state]
   [paradise.engine.matrix.spaces :as spaces]
   [paradise.engine.matrix.settings :as settings :refer [setup-encryption-listeners!]]
   [paradise.engine.matrix.timeline]
   [paradise.engine.matrix.media]
   [paradise.engine.matrix.members]
   [paradise.engine.matrix.composer]
   [paradise.engine.matrix.media-previews :as previews]
   [paradise.engine.matrix.call]
   [paradise.engine.matrix.rooms :as rooms])
  (:require-macros [paradise.shared.utils.macros :refer [export-engine]]))

(defn- get-session-class []
  (let [sdk-root (if (.-Session sdk) sdk (.-default sdk))]
    (when sdk-root
      (.-Session sdk-root))))

(defn- matrix-owned-dbs [base-name]
  [base-name
   (str base-name "::matrix-sdk-state")
   (str base-name "::matrix-sdk-crypto")
   (str base-name "::event_cache")
   (str base-name "::media")])

(defn- hydrate-session [raw-entry]
  (when raw-entry
    (when-let [Session (get-session-class)]
      {:session    (.new Session (aget raw-entry "session"))
       :raw        (aget raw-entry "session")
       :passphrase (aget raw-entry "passphrase")
       :store-id   (aget raw-entry "storeId")})))

(defn- load-hydrated-sessions []
  (p/let [store    (SessionStore. :matrix)
          sessions (.loadSessions store)]
    (into {}
          (keep (fn [uid]
                  (when-let [hydrated (hydrate-session (aget sessions uid))]
                    [uid hydrated])))
          (js/Object.keys sessions))))


(bind/register! :matrix :load-sessions
  (fn [_]
    (go
      (try
        (let [sessions (<p! (load-hydrated-sessions))
              summary  (into {}
                             (map (fn [[uid {:keys [raw]}]]
                                    [uid {:user-id        (.-userId raw)
                                          :homeserver-url (.-homeserverUrl raw)
                                          :device-id      (.-deviceId raw)}]))
                             sessions)]
          {:status :success :sessions summary})
        (catch :default e
          {:status :error :msg (str e)})))))


(defn build-client [hs passphrase? store-id? restore-or-login!]
  (p/let [sdk-root      (if (.-ClientBuilder sdk) sdk (.-default sdk))
          ClientBuilder (.-ClientBuilder sdk-root)
          SSVBuilder    (.-SlidingSyncVersionBuilder sdk-root)
          IDBBuilder    (.-IndexedDbStoreBuilder sdk-root)
          store         (SessionStore. :matrix)
          store-id      (or store-id? (.generateStoreId store))
          passphrase    (or passphrase? (.generatePassphrase store))
          store-name    (.getStoreName store store-id)
          store-config  (-> (new IDBBuilder store-name)
                            (.passphrase passphrase))
          builder       (-> (new ClientBuilder)
                            (.serverNameOrHomeserverUrl hs)
                            (.indexeddbStore store-config)
                            (.autoEnableCrossSigning true)
                            (.slidingSyncVersionBuilder (.-DiscoverNative SSVBuilder)))
          client        (.build builder)
          _             (restore-or-login! client)
          session       (.session client)
          user-id       (.-userId session)
          hs-url        (.-homeserverUrl session)
          token         (.-accessToken session)
          _             (or passphrase?
                            (.save store
                                   user-id
                                   #js {:session    session
                                        :passphrase passphrase
                                        :storeId    store-id}
                                   (matrix-owned-dbs store-name)))]
    (set-auth-context! token hs-url)
    client))

(bind/register! :matrix :bootstrap
  (fn [{:keys [target-user-id]}]
    (go
      (loop [retries 2]
        (let [[status result-or-err]
              (<p! (p/catch
                     (p/let [sessions (load-hydrated-sessions)
                             data?    (if target-user-id
                                        (get sessions target-user-id)
                                        (second (first sessions)))]
                       (if data?
                         (p/let [{:keys [session passphrase store-id]} data?
                                 hs-url  (.-homeserverUrl session)
                                 uid     (.-userId session)
                                 token   (.-accessToken session)
                                 dev-id  (.-deviceId session)
                                 client  (build-client hs-url
                                                       passphrase
                                                       store-id
                                                       #(.restoreSession % session))]
                           (reset! state/!media-cache nil)
                           (reset! state/!client client)
                           [:ok {:status :success
                                 :user-id uid
                                 :hs-url hs-url
                                 :session-data {:accessToken token
                                                :homeserverUrl hs-url
                                                :userId uid
                                                :deviceId dev-id}}])
                         [:ok {:status :empty}]))
                     (fn [e]
                       [:error e])))]
          (if (= status :error)
            (if (pos? retries)
              (do
                (<p! (p/delay 1000))
                (recur (dec retries)))
              (do
                (js/console.error "Bootstrap Exception:" result-or-err)
                {:status :error :msg (str result-or-err)}))
            result-or-err))))))

(bind/register! :matrix :login
  (fn [{:keys [homeserver username password]}]
    (go
      (try
        (<p! (p/let [client  (build-client homeserver nil nil #(.login % username password))
                     session (.session client)
                     uid     (.-userId session)
                     hs-url  (.-homeserverUrl session)
                     token   (.-accessToken session)
                     dev-id  (.-deviceId session)]
               (reset! state/!client client)
               (reset! state/!media-cache nil)
               {:status :success
                :user-id uid
                :hs-url hs-url
                :session-data {:accessToken token
                               :homeserverUrl hs-url
                               :userId uid
                               :deviceId dev-id}}))
        (catch :default e
          {:status :error :msg (str e)})))))

(bind/register! :matrix :login-action
  (fn [{:keys [action-id payload]}]
    (go
      (try
        (case (keyword action-id)
          :init-sso
          (let [hs (:homeserver payload)
                sso-url (str hs "/_matrix/client/v3/login/sso/redirect?redirectUrl="
                             (js/encodeURIComponent (.. js/globalThis -location -origin)))]
            {:status "redirect" :url sso-url})
          {:status "error" :msg (str "Unknown action-id: " action-id)})
        (catch :default e
          {:status "error" :msg (str e)})))))

(bind/register! :matrix :start-sync
                 (fn [_]
                   (go
                     (try
                       (<p! (p/let [client       @state/!client
                                    sync-service (-> (.syncService client) (.withOfflineMode) (.finish))
                                    rls-instance (.roomListService sync-service)
                                    room-list    (.allRooms rls-instance)
                                    space-service (.spaceService client)]
                              (rooms/start-room-list-sync! client room-list space-service)
                              (.start sync-service)
                              (spaces/init-space-service! client)
                              (setup-encryption-listeners! client)
                              (previews/attach-media-preview-listener! client)
                              ))
                       {:status :success}
                       (catch :default e
                         {:status :error :msg (str e)})))))

(bind/register! :matrix :fetch-profile
                 (fn [_]
                   (go
                     (try
                       (let [client  @state/!client
                             user-id (.-userId (.session client))
                             profile (<p! (.getProfile client user-id))]
                         {:status :success
                          :profile {:user-id user-id
                                    :display-name (.-displayName profile)
                                    :avatar-url   (.-avatarUrl profile)}})
                       (catch :default e
                         {:status :error :msg (str e)})))))

(bind/register! :matrix :get-client-context
                 (fn [_]
                   (go
                     (try
                       (if-let [client @state/!client]
                         (let [session (.session client)]
                           {:status :success
                            :session {:userId        (.-userId session)
                                      :deviceId      (.-deviceId session)
                                      :accessToken   (.-accessToken session)
                                      :homeserverUrl (.-homeserverUrl session)}})
                         {:status :error :msg "No active client"})
                       (catch :default e
                         {:status :error :msg (str e)})))))

(bind/register! :matrix :logout-client
  (fn [{:keys [user-id]}]
    (go
      (try
        (when-let [client @state/!client]
          (<p! (.logout client))
          (when (fn? (.-free client))
            (.free client))
          (reset! state/!client nil))

        (<p! (p/delay 1500))

        (when user-id
          (let [store (session-store/->SessionStore)]
            (<p! (.clear store user-id))
            (log/info "Worker successfully wiped OPFS session for:" user-id)))

        {:status "success"}
        (catch :default e
          (log/error "Worker Logout Panic:" e)
          {:status "error" :msg (str e)})))))


(defn preload-matrix []
  (go
    (let [raw-fetch  js/globalThis.fetch
          orig-fetch (.bind raw-fetch js/globalThis)
          wasm-url   (.-href (js/URL. "index_bg.wasm" js/globalThis.__ENGINE_URL__))]
      (try
        (set! js/globalThis.fetch
              (fn [resource & args]
                (let [url (if (string? resource) resource (.-href resource))]
                  (if (and url (str/ends-with? (str url) "index_bg.wasm"))
                    (apply orig-fetch wasm-url args)
                    (apply orig-fetch resource args)))))
        (<p! (sdk/uniffiInitAsync))
        {:status :success}
        (catch :default e
          {:status :error :msg (str e)})
        (finally
          (set! js/globalThis.fetch raw-fetch))))))

(defn ^:export bootstrap [registry-callback]
    (js/console.log "External Matrix Engine Bootstrapping...")
  (go
    (let [preload-res (<! (preload-matrix))]
      (if (= (:status preload-res) :success)
        (let [handlers (clj->js {:preload (fn [] (js/console.log "Preloaded!"))})]
          (js/console.log "WASM Initialized!")
          (registry-callback "matrix" handlers))
        (js/console.error "FATAL: Wasm failed to load:" (:msg preload-res))))))

(export-engine "matrix" bootstrap)
