(ns isaac.cli-server.ws
  "WebSocket handler for the /cli remote-CLI endpoint.

   Generalizes isaac.comm.acp.websocket/handler: upgrade the socket, then run
   the main isaac CLI dispatch with the client's handshake argv and frame the
   process IO back. HTTP auth is enforced by isaac-server before this handler.

   See PROTOCOL.md for the wire contract."
  (:require
    [cheshire.core :as json]
    [isaac.cli-server.dispatch :as dispatch]
    [isaac.logger :as log]
    [org.httpkit.server :as httpkit]))

(defn- request-client [request]
  (or (get-in request [:headers "x-forwarded-for"])
      (:remote-addr request)
      "unknown"))

(def ^:dynamic *frame-sender*
  "When bound, `(f channel json-string)` replaces httpkit/send! (handler tests)."
  nil)

(defn- send-frame! [channel frame]
  (let [payload (json/generate-string frame)]
    (if *frame-sender*
      (*frame-sender* channel payload)
      (httpkit/send! channel payload))))

(defn handler
  "Ring handler for GET /cli."
  [request]
  (if-not (:websocket? request)
    {:status  400
     :headers {"Content-Type" "text/plain"}
     :body    "websocket required"}
    (httpkit/as-channel request
      {:on-open    (fn [channel]
                     (log/debug :cli-ws/connection-opened
                                :client (request-client request)
                                :uri    (:uri request)))
       :on-close   (fn [channel _status]
                     (log/debug :cli-ws/connection-closed
                                :client (request-client request)
                                :uri    (:uri request)))
       :on-receive (fn [channel line]
                     (dispatch/receive-line! channel line #(send-frame! channel %)))})))