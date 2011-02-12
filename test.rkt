#lang racket
(require "client.rkt")

(define (send args callback)
  (define-values (in out) (listener-ports (current-listener)))
  (enqueue! callback)
  (write-bytes (encode-arguments args) out)
  (flush-output out))

(define (test)
  (current-listener (start-listener "localhost" 6379))
  (send (list #"SET" #"CAKES" #"5")
	(lambda (response)
	  (display response stdout)
	  (newline stdout)
	  (flush-output stdout))))