#lang racket
(provide (all-defined-out))

(define CRLF #"\r\n")

(define string->bytes string->bytes/utf-8)

(define bytes->string bytes->string/utf-8)

(define  (read-line* in)
  (read-bytes-line in 'return-linefeed))

(define (integer->bytes n)
  (string->bytes (number->string n)))

(define (encode-data data)
  (let ([len (integer->bytes (bytes-length data))])
    (bytes-append #"$" len CRLF data CRLF)))

(define (encode-arguments args)
  (let ([len (integer->bytes (length args))]
	[args* (map encode-data args)])
    (apply bytes-append #"*" len CRLF args*)))

(define (read-integer in)
  (string->number (read-line* in)))

(define (read-response in)
  (define sigil (integer->char (read-byte in)))
  (display (format "sigil: ~a~n" sigil))
  (case sigil
    [(#\+) (read-response/single-line in)]
    [(#\-) (read-response/error in)]
    [(#\:) (read-integer in)]
    [(#\$) (read-response/bulk in)]
    [(#\*) (read-response/multi-bulk)]
    [else (error (format "format problem in response: ~A~A" 
			 sigil
			 (read-line* in)))]
    ))

(define read-response/single-line read-line*)

(define (read-response/error in)
  (error (read-line* in)))

(define (read-response/bulk in)
  (let ([len (read-integer in)])
    (and (not (= len -1))
	 (bytes->string (read-bytes len in)))))

(define (read-crlf in)
  (unless (bytes=? (read-bytes 2 in) CRLF)
    (error "CRLF missing while reading multi-bulk")))

(define (read-response/multi-bulk in)
  (define (read-part in)
    (unless (eq? (read-byte in) #\$)
      (error "'$' missing while reading multi-bulk reply"))
    
    (let* ([len (read-integer in)]
           [msg (and (not (= len -1))
                     (bytes->string (read-bytes len in)))])
      (read-crlf in)
      msg))

  (let loop ([n (read-integer in)]
             [ls '()])
    (if (= n 0)
        (reverse ls)
        (loop (- n 1)
              (cons (read-part in) ls)))))