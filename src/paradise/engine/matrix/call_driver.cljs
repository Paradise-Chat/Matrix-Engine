(ns paradise.engine.matrix.call-driver
  (:require
   ["livekit-client" :as lkc]
   [clojure.string :as str]
   [promesa.core :as p]))

(defn- decode-base64-key [key-b64-string]
  (let [std-b64    (-> key-b64-string
                       (str/replace "-" "+")
                       (str/replace "_" "/"))
        remainder  (mod (.-length std-b64) 4)
        padded-b64 (if (zero? remainder)
                     std-b64
                     (str std-b64 (str/join (repeat (- 4 remainder) "="))))
        raw-str    (js/atob padded-b64)
        length     (.-length raw-str)
        key-buffer (js/Uint8Array. length)]
    (dotimes [i length]
      (aset key-buffer i (.charCodeAt raw-str i)))
    key-buffer))

(defn- microphone-capture-options []
  #js {:echoCancellation true
       :noiseSuppression false
       :autoGainControl true
       :channelCount 1
       :sampleRate 48000})

(defn- extract-matrix-user-id [identity-str]
  (when (and identity-str (not (str/blank? identity-str)))
    (or (re-find #"^@[^:]+:[^:|]+" (str identity-str))
        (str identity-str))))

(defn- collect-participant-tiles [^js room]
  (if-not room
    #js []
    (let [tiles      (js/Array.)
          mic-source (.. ^js lkc -Track -Source -Microphone)
          cam-source (.. ^js lkc -Track -Source -Camera)
          scr-source (.. ^js lkc -Track -Source -ScreenShare)
          add-p!     (fn [^js p local?]
                       (when p
                         (let [pid            (or (not-empty (.-identity p)) "unknown")
                               uid            (or (extract-matrix-user-id pid) pid)
                               dname          (or (not-empty (.-name p)) uid)
                               speaking?      (boolean (.-isSpeaking p))
                               ^js mic-pub    (.getTrackPublication p mic-source)
                               ^js cam-pub    (.getTrackPublication p cam-source)
                               ^js scr-pub    (.getTrackPublication p scr-source)
                               ^js mic-t      (some-> mic-pub .-track)
                               ^js cam-t      (some-> cam-pub .-track)
                               ^js scr-t      (some-> scr-pub .-track)
                               mic-track      (some-> mic-t .-mediaStreamTrack)
                               cam-track      (some-> cam-t .-mediaStreamTrack)
                               scr-track      (some-> scr-t .-mediaStreamTrack)
                               audio-muted    (if mic-pub (boolean (.-isMuted mic-pub)) true)
                               video-muted    (if cam-pub (boolean (.-isMuted cam-pub)) true)]
                           (.push tiles
                                  #js {"id"             (str pid ":camera")
                                       "participant-id" pid
                                       "user-id"        uid
                                       "display-name"   dname
                                       "source"         "camera"
                                       "local?"         local?
                                       "speaking?"      speaking?
                                       "audio-muted?"   audio-muted
                                       "video-muted?"   video-muted
                                       "video-track"    cam-track
                                       "audio-track"    mic-track})
                           (when (and scr-pub scr-track (not (.-isMuted scr-pub)))
                             (.push tiles
                                    #js {"id"             (str pid ":screen-share")
                                         "participant-id" pid
                                         "user-id"        uid
                                         "display-name"   (str dname " (Screen)")
                                         "source"         "screen-share"
                                         "local?"         local?
                                         "speaking?"      false
                                         "audio-muted?"   true
                                         "video-muted?"   false
                                         "video-track"    scr-track
                                         "audio-track"    nil})))))]
      (add-p! (.-localParticipant room) true)
      (when-let [^js remotes (.-remoteParticipants room)]
        (.forEach remotes (fn [p] (add-p! p false))))
      tiles)))

(defn ^:export createCallDriver [^js bridge]
  (let [session-cfg     (.-sessionConfig bridge)
        sfu-url         (aget session-cfg "url")
        sfu-token       (aget session-cfg "token")
        encoder-worker  (.-encoderWorker bridge)
        get-processor   (.-getAudioProcessor bridge)
        on-participants (.-onParticipantsChanged bridge)
        on-local-media  (.-onLocalMediaChanged bridge)
        on-connected    (.-onConnected bridge)
        on-disconnected (.-onDisconnected bridge)
        on-error        (.-onError bridge)
        ^js kp          (when encoder-worker
                          (new (.-BaseKeyProvider ^js lkc)
                               #js {:ratchetWindowSize 10 :keyringSize 256}))
        room-opts       (cond-> {:audioCaptureDefaults (microphone-capture-options)
                                 :videoCaptureDefaults #js {:resolution (.. ^js lkc -VideoPresets -hd1080)}
                                 :publishDefaults      #js {:simulcast false :videoCodec "vp8"}}
                          (and kp encoder-worker)
                          (assoc :e2ee #js {:keyProvider kp :worker encoder-worker}))
        ^js room        (new (.-Room ^js lkc) (clj->js room-opts))
        !known-keys     (atom {})
        sync-all!       (fn [& _]
                          (when on-participants
                            (on-participants (collect-participant-tiles room)))
                          (when-let [^js lp (.-localParticipant room)]
                            (when on-local-media
                              (on-local-media
                               #js {:audio          (boolean (.-isMicrophoneEnabled lp))
                                    :video          (boolean (.-isCameraEnabled lp))
                                    :screen-sharing (boolean (.-isScreenShareEnabled lp))}))))
        attach-proc!    (fn [^js ltrack]
                          (when (and ltrack get-processor)
                            (when-not (.getProcessor ltrack)
                              (when-let [proc (get-processor)]
                                (-> (.setProcessor ltrack proc)
                                    (p/catch (fn [e]
                                               (when on-error
                                                 (on-error "processor" e)))))))))]

    (let [^js ev (.-RoomEvent ^js lkc)]
      (doseq [e [(.-ParticipantConnected ev)
                 (.-ParticipantDisconnected ev)
                 (.-TrackSubscribed ev)
                 (.-TrackUnsubscribed ev)
                 (.-TrackMuted ev)
                 (.-TrackUnmuted ev)
                 (.-LocalTrackPublished ev)
                 (.-LocalTrackUnpublished ev)
                 (.-ActiveSpeakersChanged ev)]]
        (.on room e sync-all!))

      (.on room (.-LocalTrackPublished ev)
           (fn [^js pub _]
             (when (= (.-source pub) (.. ^js lkc -Track -Source -Microphone))
               (attach-proc! (.-track pub)))))

      (.on room (.-Disconnected ev)
           (fn [reason]
             (when on-disconnected
               (on-disconnected reason)))))

    #js {:connect
         (fn []
           (-> (p/let [_ (.connect room sfu-url sfu-token)
                       _ (if (and kp encoder-worker)
                           (.setE2EEEnabled room true)
                           (p/resolved nil))
                       ^js lp (.-localParticipant room)
                       ^js pub (.setMicrophoneEnabled lp true (microphone-capture-options))]
                 (when-let [^js t (some-> pub .-track)]
                   (attach-proc! t))
                 (sync-all!)
                 (when on-connected
                   (on-connected)))
               (p/catch (fn [err]
                          (when on-error
                            (on-error "connect" err))))))

         :disconnect
         (fn []
           (.disconnect room))

         :setMicrophoneEnabled
         (fn [enabled?]
           (if-let [^js lp (.-localParticipant room)]
             (p/let [^js pub (.setMicrophoneEnabled lp (boolean enabled?) (microphone-capture-options))]
               (when (and enabled? pub)
                 (attach-proc! (.-track pub)))
               (sync-all!)
               pub)
             (p/rejected (js/Error. "No local participant"))))

         :setCameraEnabled
         (fn [enabled?]
           (if-let [^js lp (.-localParticipant room)]
             (p/let [pub (.setCameraEnabled lp (boolean enabled?))]
               (sync-all!)
               pub)
             (p/rejected (js/Error. "No local participant"))))

         :setScreenShareEnabled
         (fn [enabled?]
           (if-let [^js lp (.-localParticipant room)]
             (p/let [pub (.setScreenShareEnabled lp (boolean enabled?))]
               (sync-all!)
               pub)
             (p/rejected (js/Error. "No local participant"))))

         :switchDevice
         (fn [kind device-id]
           (.switchActiveDevice room kind device-id))

         :injectCryptoMaterial
         (fn [^js payload]
           (let [pid  (or (aget payload "participant-id") (aget payload "participantId"))
                 kidx (or (aget payload "key-index") (aget payload "keyIndex") 0)
                 kb64 (or (aget payload "key-array") (aget payload "keyArray"))]
             (if (and pid kb64)
               (let [kbuf (decode-base64-key kb64)]
                 (p/let [ckey (js/crypto.subtle.importKey
                               "raw"
                               (.-buffer kbuf)
                               "HKDF"
                               false
                               #js ["deriveBits" "deriveKey"])]
                   (swap! !known-keys assoc-in [pid kidx] ckey)
                   (when kp
                     (.onSetEncryptionKey kp ckey pid (js/Number. kidx)))
                   ckey))
               (p/resolved nil))))}))
