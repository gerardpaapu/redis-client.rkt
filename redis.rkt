#lang racket

(define-struct redis-client (url port in out))

(define (make-redis-client* url port)
  (define-values (in out) (tcp-connect url port))
  (make-redis-client url port in out))

(define CRLF #"\r\n")

(define (encode-data data)
  (bytes-append #"$" (integer->bytes (bytes-length data)) CRLF
                data CRLF))

(define (encode-arguments args)
  (apply bytes-append #"*"
         (integer->bytes (length args)) CRLF
         (map encode-data args)))

(define (integer->bytes n)
  (string->bytes/latin-1 (number->string n)))

(define (read-integer in)
  (string->number (read-line in 'return-linefeed)))

(define (read-response in)
  (case (read-byte in)
    [(#\+) (read-response/single-line in)]
    [(#\-) (read-response/error in)]
    [(#\:) (read-integer in)]
    [(#\$) (read-response/bulk in)]
    [(#\*) (read-response/multi-bulk)]
    [else (error "format problem in response")]))

(define (read-response/single-line in)
  (read-line in 'return-linefeed))

(define (read-response/error in)
  (error (read-response/single-line in)))

(define (read-response/bulk in)
  (let ([len (read-integer in)])
    (and (not (= len -1))
	 (bytes->string/utf-8 (read-bytes len in)))))

(define (read-response/multi-bulk in)
  (define (read-crlf in)
    (unless (bytes=? (read-bytes 2 in) CRLF)
      (error "CRLF missing while reading multi-bulk")))

  (define (read-part in)
    (unless (eq? (read-byte in) #\$)
      (error "'$' missing while reading multi-bulk reply"))
    
    (let* ([len (read-integer in)]
           [msg (and (not (= len -1))
                     (bytes->string/utf-8 (read-bytes len in)))])
      (read-crlf in)
      msg))

  (let loop ([n (read-integer in)]
             [ls '()])
    (if (= n 0)
        (reverse ls)
        (loop (- n 1)
              (cons (read-part in) ls)))))
