#lang racket

(require "protocol.rkt")

(provide (all-defined-out)
	 (all-from-out "protocol.rkt"))

(define-struct listener (in out 
			    [queue        #:mutable] 
			    [subscribers  #:mutable]
			    [psubscribers #:mutable]
			    custodian))

(define stdout (current-output-port))

(define current-listener (make-parameter #f))

(define (listener-ports listener)
  (values (listener-in listener)
	  (listener-out listener)))

(define (start-listener url port)
  (define-values (in out) (tcp-connect url port))
  (define cust (make-custodian))
  (define lsnr (make-listener in out '() #hash() #hash() cust))
  (parameterize ([current-custodian cust]
		 [current-listener  lsnr])
    (thread (lambda ()
	      (define (loop)
		(define-values (in out) (listener-ports (current-listener)))
		(sync (peek-bytes-evt 1 0 #f in))
		(unless (eof-object? (peek-byte in))
		  ;; If we see <eof> it means
		  ;; we've been disconnected from the server
		  ;; I should probably handle that somehow
		  (handle (read-response in))
		  (loop)))
	      (loop))))
  lsnr)

(define (queue-empty?)
  (empty? (listener-queue (current-listener))))

(define (dequeue!)
  (let* ([listener (current-listener)]
	 [q (listener-queue listener)]
	 [q* (reverse q)]
	 [item (first q*)])
    (set-listener-queue! listener (rest q*))
    item))

(define (enqueue! callback)
  (let* ([listener (current-listener)]
	 [q (listener-queue listener)])
    (set-listener-queue! listener
			 (cons callback q))))

(define (add-subscriber! channel callback)
  ;; this just registers you with the listener's table
  ;; you still need to send the subscribe command to the 
  ;; redis server
  (let* ([listener (current-listener)]
	 [subs (listener-subscribers listener)]
	 [callbacks (dict-ref subs channel '())]
	 [callbacks* (cons callback callbacks)])
    (set-listener-subscribers! (dict-set subs callbacks*))))

(define (add-psubscriber! channel callback)
  ;; this just registers you with the listener's table
  ;; you still need to send the psubscribe command to the 
  ;; redis server
  (let* ([listener (current-listener)]
	 [subs (listener-psubscribers listener)]
	 [callbacks (dict-ref subs channel '())]
	 [callbacks* (cons callback callbacks)])

    (set-listener-psubscribers! (dict-set subs callbacks*))))

(define (handle response)
  (match-define (struct* listener ([queue queue]
				   [subscribers subscribers]
				   [psubscribers psubscribers]))
		(current-listener))

  (cond [(message? response)
	 (map (lambda (callback)
		(callback response))
	      (dict-ref subscribers
			(message-channel response)))]

	[(pmessage? response)
	 (map (lambda (callback)
		(callback response))
	      (dict-ref psubscribers
			(message-channel response)))]

	[(not (queue-empty?)) ((dequeue!) response)]

	[else  (error (format "dispatch failed on ~a" response))]))

(define (message? response)
  (and (list? response) 
       (bytes=? #"message" (first response))))

(define (pmessage? response)
  (and (list? response)
       (bytes=? #"pmessage" (first response))))

(define (message-channel response)
  (second response))

;; (define connection (make-redis-connection* "localhost" 6379))
;; (define in (redis-connection-in connection))
;; (define out (redis-connection-out connection))
;; (write-bytes (encode-arguments '(#"PING")) out)
;; (read-response in)

