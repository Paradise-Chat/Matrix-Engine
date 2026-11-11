(ns paradise.engine.matrix.media
  (:require [clojure.string :as str]
            [promesa.core :as p]
            [net :as net]
            [paradise.shared.client.session-store :as store :refer [SessionStore]]
            [paradise.engine.binding :as bind]
            [cljs.core.async.interop :refer-macros [<p!]]
            [cljs.core.async :refer [go]]))

(defonce !media-sessions (atom {}))
(defonce !active-user-id (atom nil))
(defonce !hydrating-promises (atom {}))

(defn- normalize-server-url [url]
  (when (and url (not (str/blank? (str url))))
    (-> (net/ensure-protocol url)
        str
        (str/replace #"/+$" ""))))

(defn- http-url? [uri]
  (let [s (str uri)]
    (or (str/starts-with? s "http://")
        (str/starts-with? s "https://"))))

(defn- passthrough-uri? [uri]
  (let [s (str uri)]
    (or (str/starts-with? s "data:")
        (str/starts-with? s "blob:"))))

(defn- matrix-server-http-url? [server raw-url]
  (when (and (not (str/blank? server)) (http-url? raw-url))
    (try
      (let [u (js/URL. (str raw-url))
            s (js/URL. (str server))]
        (and (= (.-origin u) (.-origin s))
             (str/includes? (.-pathname u) "/_matrix/")))
      (catch :default _ false))))

(defn- external-http-url? [server raw-uri]
  (and (http-url? raw-uri)
       (not (matrix-server-http-url? server raw-uri))))

(defn- apply-media-session! [{:keys [user-id server-url base-url token]}]
  (let [clean-hs (normalize-server-url (or server-url base-url))]
    (when (and user-id (not (str/blank? clean-hs)) (not (str/blank? (str token))))
      (let [entry {:user-id  user-id
                   :base-url clean-hs
                   :token    token}]
        (swap! !media-sessions assoc user-id entry)
        (reset! !active-user-id user-id)
        (net/set-auth-context! token clean-hs)
        entry))))

(defn- clear-media-session! [{:keys [user-id all?]}]
  (if (or all? (nil? user-id))
    (do
      (reset! !media-sessions {})
      (reset! !active-user-id nil)
      (net/set-auth-context! nil nil))
    (do
      (swap! !media-sessions dissoc user-id)
      (when (= @!active-user-id user-id)
        (reset! !active-user-id nil)
        (net/set-auth-context! nil nil)))))

(defn- read-store-session [target-user-id]
  (p/let [saved-uid (if target-user-id
                      nil
                      (-> (store/get-setting "last_active_user")
                          (p/catch (fn [_] nil))))
          want-uid  (or target-user-id saved-uid)
          s-store   (SessionStore. :matrix)
          sessions  (.loadSessions s-store)
          uids      (when sessions (js/Object.keys sessions))
          uid       (or (when (and want-uid sessions (aget sessions want-uid))
                          want-uid)
                        (when (and (nil? target-user-id) uids (pos? (.-length uids)))
                          (aget uids 0)))
          entry     (when uid (aget sessions uid))
          raw       (when entry (or (aget entry "session") entry))
          base-url  (when raw (or (.-homeserverUrl raw)
                                  (.-homeserver_url raw)
                                  (.-serverUrl raw)))
          token     (when raw (or (.-accessToken raw)
                                  (.-access_token raw)
                                  (.-token raw)))
          clean-hs  (normalize-server-url base-url)]
    (when (and uid clean-hs token)
      (apply-media-session! {:user-id  uid
                             :base-url clean-hs
                             :token    token}))))

(defn- ensure-media-session! [target-user-id]
  (let [lookup-uid (or target-user-id @!active-user-id)
        cached     (when lookup-uid (get @!media-sessions lookup-uid))]
    (if cached
      (do
        (when (not= @!active-user-id (:user-id cached))
          (reset! !active-user-id (:user-id cached)))
        (net/set-auth-context! (:token cached) (:base-url cached))
        (p/resolved cached))
      (let [flight-key (or lookup-uid ::default)]
        (if-let [in-flight (get @!hydrating-promises flight-key)]
          in-flight
          (let [p-inst (-> (p/loop [retries 6]
                             (p/let [res (read-store-session target-user-id)]
                               (if (or res (<= retries 0))
                                 res
                                 (p/do!
                                   (p/delay 250)
                                   (p/recur (dec retries))))))
                           (p/finally
                             (fn []
                               (swap! !hydrating-promises dissoc flight-key))))]
            (swap! !hydrating-promises assoc flight-key p-inst)
            p-inst))))))

(defn b64->uint8 [b64]
  (let [b1     (str/replace b64 "-" "+")
        b2     (str/replace b1 "_" "/")
        pad    (mod (- 4 (mod (count b2) 4)) 4)
        padded (str b2 (apply str (repeat pad "=")))
        raw    (js/atob padded)
        len    (.-length raw)
        arr    (js/Uint8Array. len)]
    (dotimes [i len]
      (aset arr i (.charCodeAt raw i)))
    arr))

(defn- strip-matrix-uri [uri]
  (-> (str uri)
      (str/replace #"^mxc://" "")
      (str/replace #"^emote://matrix/" "")
      (str/replace #"^/+" "")))

(defn- build-media-url [server uri {:keys [width height method]}]
  (if (http-url? uri)
    (str/replace (str uri) #"([^:])//+_matrix/" "$1/_matrix/")
    (let [clean-id (strip-matrix-uri uri)]
      (if (and width height)
        (str server "/_matrix/client/v1/media/thumbnail/" clean-id
             "?width=" (int width)
             "&height=" (int height)
             "&method=" (or method "scale"))
        (str server "/_matrix/client/v1/media/download/" clean-id)))))

(defn- retryable-status? [status]
  (contains? #{429 502 503 504} status))

(defn- fetch-with-retry! [url opts retries authenticated?]
  (let [exec-fetch (fn []
                     (if authenticated?
                       (net/fetch url opts)
                       (js/fetch url opts)))]
    (p/loop [attempt 0]
      (-> (exec-fetch)
          (p/then
           (fn [resp]
             (if (or (.-ok resp)
                     (>= attempt retries)
                     (not (retryable-status? (.-status resp))))
               resp
               (let [retry-after (some-> (.-headers resp)
                                         (.get "Retry-After")
                                         js/parseFloat)
                     delay-ms    (if (and retry-after (not (js/isNaN retry-after)))
                                   (* retry-after 1000)
                                   (* 450 (inc attempt)))]
                 (p/do!
                   (p/delay delay-ms)
                   (p/recur (inc attempt)))))))
          (p/catch
           (fn [err]
             (if (>= attempt retries)
               (p/rejected err)
               (p/do!
                 (p/delay (* 450 (inc attempt)))
                 (p/recur (inc attempt))))))))))

(defn- fetch-media-response! [server raw-uri thumb-opts token]
  (let [primary-url  (build-media-url server raw-uri thumb-opts)
        has-thumb?   (and (:width thumb-opts) (:height thumb-opts) (not (http-url? raw-uri)))
        fallback-url (when has-thumb?
                       (build-media-url server raw-uri nil))
        auth?        (and (boolean token) (matrix-server-http-url? server primary-url))
        fetch-opts   (if auth?
                       #js {:headers #js {"Authorization" (str "Bearer " token)}}
                       #js {:credentials "omit"})]
    (-> (fetch-with-retry! primary-url fetch-opts 2 auth?)
        (p/then
         (fn [resp]
           (if (or (.-ok resp) (not fallback-url))
             resp
             (fetch-with-retry! fallback-url fetch-opts 1 auth?))))
        (p/catch
         (fn [err]
           (if fallback-url
             (fetch-with-retry! fallback-url fetch-opts 1 auth?)
             (p/rejected err)))))))

(bind/register!
 :matrix :set-media-session
 (fn [{:keys [user-id server-url base-url token]}]
   (go
     (if-let [applied (apply-media-session! {:user-id    user-id
                                             :server-url server-url
                                             :base-url   base-url
                                             :token      token})]
       {:status   "success"
        :user-id  (:user-id applied)
        :base-url (:base-url applied)
        :token    (:token applied)}
       (if user-id
         (if-let [hydrated (<p! (ensure-media-session! user-id))]
           {:status   "success"
            :user-id  (:user-id hydrated)
            :base-url (:base-url hydrated)
            :token    (:token hydrated)}
           {:status "empty"})
         {:status "ignored"})))))

(bind/register!
 :matrix :clear-media-session
 (fn [opts]
   (go
     (clear-media-session! (or opts {}))
     {:status "success"})))

(bind/register!
 :matrix :get-media
 (fn [{:keys [source-map uri source user-id width height method download?]}]
   (go
     (let [sm-clj     (when source-map
                        (if (map? source-map)
                          source-map
                          (js->clj source-map :keywordize-keys true)))
           file-info  (:file sm-clj)
           target-uri (or uri
                          source
                          (:url sm-clj)
                          (:uri sm-clj)
                          (get-in sm-clj [:plain :url]))
           thumb-opts {:width width :height height :method method}]
       (cond
         (not (or file-info (and target-uri (not (str/blank? (str target-uri))))))
         {:status "ignored" :msg "No source provided"}

         (and (not file-info) (passthrough-uri? target-uri))
         {:status "direct" :url (str target-uri)}

         :else
         (try
           (let [{:keys [base-url token] resolved-uid :user-id} (<p! (ensure-media-session! user-id))
                 server                                         (normalize-server-url base-url)]
             (cond
               file-info
               (let [file-url (:url file-info)]
                 (if (and (not (http-url? file-url)) (str/blank? server))
                   {:status "error" :msg "No active Matrix session found in SessionStore"}
                   (let [jwk  (clj->js (:key file-info))
                         iv   (:iv file-info)
                         resp (<p! (fetch-media-response! server file-url nil token))]
                     (when (= (.-status resp) 401)
                       (clear-media-session! {:user-id resolved-uid}))
                     (if-not (.-ok resp)
                       (throw (ex-info "Fetch failed" {:status (.-status resp)}))
                       (let [enc-buf    (<p! (.arrayBuffer resp))
                             iv-arr     (b64->uint8 iv)
                             algo       #js {:name "AES-CTR"}
                             crypto-key (<p! (js/crypto.subtle.importKey "jwk" jwk algo false #js ["decrypt"]))
                             dec-buf    (<p! (js/crypto.subtle.decrypt
                                              #js {:name "AES-CTR" :counter iv-arr :length 64}
                                              crypto-key
                                              enc-buf))]
                         {:status "success" :bytes dec-buf :transfer [:bytes]})))))

               (external-http-url? server target-uri)
               (let [clean-url (str target-uri)]
                 (if download?
                   (try
                     (let [resp (<p! (fetch-with-retry! clean-url #js {:credentials "omit"} 1 false))]
                       (if (.-ok resp)
                         (let [buf (<p! (.arrayBuffer resp))]
                           {:status "success" :bytes buf :transfer [:bytes]})
                         {:status "direct" :url clean-url}))
                     (catch :default _
                       {:status "direct" :url clean-url}))
                   {:status "direct" :url clean-url}))

               (str/blank? server)
               {:status "error" :msg "No active Matrix session found in SessionStore"}

               :else
               (let [resp (<p! (fetch-media-response! server target-uri thumb-opts token))]
                 (when (= (.-status resp) 401)
                   (clear-media-session! {:user-id resolved-uid}))
                 (if-not (.-ok resp)
                   (throw (ex-info "Fetch failed" {:status (.-status resp)}))
                   (let [buf (<p! (.arrayBuffer resp))]
                     {:status "success" :bytes buf :transfer [:bytes]})))))
           (catch :default e
             (js/console.warn "Media worker trapped fetch error:" e)
             {:status "error" :msg (str e)})))))))
