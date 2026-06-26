(ns isaac.cli-server.ws
  "WebSocket handler for the /cli remote-CLI endpoint.

   Generalizes isaac.comm.acp.websocket/handler: authenticate at the HTTP
   upgrade, then — instead of speaking ACP JSON-RPC — run the main isaac CLI
   dispatch with the client's handshake argv, piping the process's
   stdin/stdout/stderr and exit code over the socket as framed JSON messages.

   See PROTOCOL.md for the wire contract.")

;; ---------------------------------------------------------------------------
;; M1 TODO:
;;   - require :websocket? on the ring request; reject non-ws with 400.
;;   - auth at upgrade (reuse the server's bearer-token check, as /acp does);
;;     reject unauthenticated with 401, no socket.
;;   - on {:type "start" :argv [...] :cwd ...}: run the dispatch, stream
;;     {:type "stdout" :data <b64>} frames, then {:type "exit" :code N}; close.
;;   - empty argv -> usage on stdout + exit 0.
;; ---------------------------------------------------------------------------

(defn handler
  "Ring handler for GET /cli. Placeholder until M1."
  [_request]
  {:status  501
   :headers {"content-type" "text/plain"}
   :body    "isaac-cli-server: /cli not implemented yet (M1)"})
