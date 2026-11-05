(ns build
  (:require [clojure.tools.build.api :as b]
            [clojure.java.io :as io]))

(def crypto-bundle-path
  "node_modules/paradise-matrix-push-crypto/dist/paradise_matrix_push_crypto_bundle.js")

(def worker-Temp-path
  "dist-push/push-worker.js")

(def push-out-path
  "dist/push-matrix.js")

(defn ensure-node-modules! []
  (when-not (.exists (io/file "node_modules"))
    (b/process {:command-args ["pnpm" "install"]})))

(defn build-push-bundle! [force?]
  (when (or force? (not (.exists (io/file push-out-path))))
    (println "Building push-worker and combining dist/push-matrix.js...")
    (b/process {:command-args ["npx" "shadow-cljs" "release" "push-worker"]})
    (let [crypto-js (slurp crypto-bundle-path)
          worker-js (slurp worker-Temp-path)]
      (io/make-parents push-out-path)
      (spit push-out-path (str crypto-js "\n" worker-js)))))

(defn build-call-workers! [force?]
  (when (or force?
            (not (.exists (io/file "dist/call-driver.js")))
            (not (.exists (io/file "dist/call-encoder.js"))))
    (println "Building call-driver and call-encoder bundles...")
    (b/process {:command-args ["pnpm" "run" "build:driver-encoder"]})))

(defn prep [{:keys [force]}]
  (ensure-node-modules!)
  (build-push-bundle! force)
  (build-call-workers! force))
