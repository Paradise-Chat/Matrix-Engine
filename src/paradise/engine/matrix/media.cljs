(ns paradise.engine.matrix.media
  (:require [clojure.string :as str]
            [promesa.core :as p]
            [net :as net]
            [paradise.shared.client.session-store :refer [SessionStore]]
            [paradise.engine.binding :as bind]
            [cljs.core.async.interop :refer-macros [<p!]]
            [cljs.core.async :refer [go]]))

(defonce !media-session (atom nil))
(defonce !hydrating-promise (atom nil))

(defn- normalize-server-url [url]
  (when (and url (not (str/blank? (str url))))
    (-> (net/ensure-protocol url)
        str
        (str/replace #"/+$" ""))))

(defn- read-store-session [target-user-id]
  (p/let [store    (SessionStore. :matrix)
          sessions (.loadSessions store)
          uids     (when sessions (js/Object.keys sessions))
          uid      (or (when (and target-user-id sessions (aget sessions target-user-id))
                         target-user-id)
                       (when (and uids (pos? (.-length uids)))
                         (aget uids 0)))
          entry    (when uid (aget sessions uid))
          raw      (when entry (or (aget entry "session") entry))
          base-url (when raw (or (.-homeserverUrl raw) (.-homeserver_url raw)))
          token    (when raw (or (.-accessToken raw) (.-access_token raw)))
          clean-hs (normalize-server-url base-url)]
    (when (and clean-hs token)
      (let [s {:base-url clean-hs
               :token    token}]
        (reset! !media-session s)
        (net/set-auth-context! token clean-hs)
        s))))

(defn- ensure-media-session! [target-user-id]
  (if-let [existing @!media-session]
    (p/resolved existing)
    (if-let [in-flight @!hydrating-promise]
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
                           (reset! !hydrating-promise nil))))]
        (reset! !hydrating-promise p-inst)
        p-inst))))

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
  (if (or (str/starts-with? (str uri) "http://")
          (str/starts-with? (str uri) "https://"))
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

(defn- fetch-with-retry! [url opts retries]
  (p/loop [attempt 0]
    (-> (net/fetch url opts)
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
               (p/recur (inc attempt)))))))))

(defn- fetch-media-response! [server raw-uri thumb-opts fetch-opts]
  (let [primary-url  (build-media-url server raw-uri thumb-opts)
        has-thumb?   (and (:width thumb-opts) (:height thumb-opts))
        fallback-url (when has-thumb?
                       (build-media-url server raw-uri nil))]
    (-> (fetch-with-retry! primary-url fetch-opts 2)
        (p/then
         (fn [resp]
           (if (or (.-ok resp) (not fallback-url))
             resp
             (fetch-with-retry! fallback-url fetch-opts 1))))
        (p/catch
         (fn [err]
           (if fallback-url
             (fetch-with-retry! fallback-url fetch-opts 1)
             (p/rejected err)))))))

(bind/register!
 :matrix :get-media
 (fn [{:keys [source-map uri source user-id width height method]}]
   (go
     (let [target-uri (or uri source)
           thumb-opts {:width width :height height :method method}]
       (if-not (or source-map target-uri)
         {:status "ignored" :msg "No source provided"}
         (try
           (let [{:keys [base-url token]} (<p! (ensure-media-session! user-id))
                 server                   (normalize-server-url base-url)]
             (if (str/blank? server)
               {:status "error" :msg "No active Matrix session found in SessionStore"}
               (let [fetch-opts (if token
                                  #js {:headers #js {"Authorization" (str "Bearer " token)}}
                                  #js {})]
                 (cond
                   source-map
                   (let [sm-clj    (if (map? source-map)
                                     source-map
                                     (js->clj source-map :keywordize-keys true))
                         file-info (:file sm-clj)]
                     (if-not file-info
                       (let [raw-uri (or target-uri (:url sm-clj) (get-in sm-clj [:plain :url]) "")
                             resp    (<p! (fetch-media-response! server raw-uri thumb-opts fetch-opts))]
                         (if-not (.-ok resp)
                           (throw (ex-info "Fetch failed" {:status (.-status resp)}))
                           (let [buf (<p! (.arrayBuffer resp))]
                             {:status "success" :bytes buf :transfer [:bytes]})))
                       (let [jwk  (clj->js (:key file-info))
                             iv-b64 (:iv file-info)
                             resp (<p! (fetch-media-response! server (:url file-info) nil fetch-opts))]
                         (if-not (.-ok resp)
                           (throw (ex-info "Fetch failed" {:status (.-status resp)}))
                           (let [enc-buf    (<p! (.arrayBuffer resp))
                                 iv-arr     (b64->uint8 iv-b64)
                                 algo       #js {:name "AES-CTR"}
                                 crypto-key (<p! (js/crypto.subtle.importKey "jwk" jwk algo false #js ["decrypt"]))
                                 dec-buf    (<p! (js/crypto.subtle.decrypt
                                                  #js {:name "AES-CTR" :counter iv-arr :length 64}
                                                  crypto-key
                                                  enc-buf))]
                             {:status "success" :bytes dec-buf :transfer [:bytes]})))))

                   target-uri
                   (let [resp (<p! (fetch-media-response! server target-uri thumb-opts fetch-opts))]
                     (if-not (.-ok resp)
                       (throw (ex-info "Fetch failed" {:status (.-status resp)}))
                       (let [buf (<p! (.arrayBuffer resp))]
                         {:status "success" :bytes buf :transfer [:bytes]})))

                   :else
                   {:status "error" :msg "Neither source-map nor URI provided."}))))
           (catch :default e
             (js/console.warn "Media worker trapped fetch error:" e)
             {:status "error" :msg (str e)})))))))
