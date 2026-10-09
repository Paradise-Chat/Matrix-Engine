(ns paradise.engine.matrix.core
  (:require
   [paradise.engine.binding :as bind]
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
   [paradise.engine.matrix.members]
   [paradise.engine.matrix.composer]
   [paradise.engine.matrix.media-previews :as previews]
   [paradise.engine.matrix.call]
   [paradise.engine.matrix.rooms :as rooms]
   )
  (:require-macros [paradise.shared.utils.macros :refer [export-engine]]))

(defn maybe-local-session []
  (p/let [store (SessionStore.)
          sessions (.loadSessions store)
          user-id (first (js/Object.keys sessions))]
    (when user-id (aget sessions user-id))))

(defn get-specific-session [target-user-id]
  (p/let [store (SessionStore.)
          sessions (.loadSessions store)]
    (aget sessions target-user-id)))

(defn build-client [hs passphrase? store-id? restore-or-login!]
  (p/let [sdk-root (if (.-ClientBuilder sdk) sdk (.-default sdk))
          ClientBuilder (.-ClientBuilder sdk-root)
          SSVBuilder    (.-SlidingSyncVersionBuilder sdk-root)
          IDBBuilder    (.-IndexedDbStoreBuilder sdk-root)
          store (SessionStore.)
          store-id   (or store-id? (.generateStoreId store))
          passphrase (or passphrase? (.generatePassphrase store))
          store-name (.getStoreName store store-id)
          store-config (-> (new IDBBuilder store-name)
                           (.passphrase passphrase))
          builder (-> (new ClientBuilder)
                      (.serverNameOrHomeserverUrl hs)
                      (.indexeddbStore store-config)
                      (.autoEnableCrossSigning true)
                      (.slidingSyncVersionBuilder (.-DiscoverNative SSVBuilder)))
          client  (.build builder)
          _ (restore-or-login! client)
          session (.session client)
          _ (or passphrase? (.save store session passphrase store-id))]
     client))


(bind/register! :matrix :bootstrap
  (fn [{:keys [target-user-id]}]
    (go
      (loop [retries 2]
        (let [[status result-or-err]
              (<p! (p/catch
                     (p/let [data? (if target-user-id
                                     (get-specific-session target-user-id)
                                     (maybe-local-session))]
                       (if data?
                         (p/let [session (.-session data?)
                                 hs-url  (.-homeserverUrl session)
                                 uid     (.-userId session)
                                 token   (.-accessToken session)
                                 dev-id  (.-deviceId session)
                                 client  (build-client hs-url
                                                       (.-passphrase data?)
                                                       (.-storeId data?)
                                                       #(.restoreSession % session))]
                           (reset! state/!media-cache nil)
                           (reset! state/!client client)
                           (set-auth-context! token hs-url)
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
  (fn [{:keys [homeserver username password] :as payload}]
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
    (try
      (<p! (sdk/uniffiInitAsync "https://paradise-chat.github.io/Matrix-Engine/index_bg.wasm"))
      {:status :success}
      (catch :default e
        {:status :error :msg (str e)}))))

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
