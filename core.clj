(ns cdupes.core
  (require digest)
  (:import (java.io RandomAccessFile))
  (:require [clojure.tools.cli :refer [parse-opts]]
            [clojure.java.io :refer [file]]
            [clojure.string :as string])
  (:gen-class))

(def cli-options
  [["-s" "--size SIZE" "Only run on files of certain sizes, e.g. -1G +200M 500K"]
   ["-r" "--recurse" "Recurse into subdirectories"]
   ["-R" "--regex REGEX" "Regular expression to match files"]
   ["-i" "--invert-regex" "Only run on files not matching --regex"]
   ["-p" "--precision PRECISION" "Levels of precision: 0 - Consider equal length files identical
                                                     1 - Consider equal MD5 Checksum files identical
                                                     2 - Consider byte-by-byte equal files identical (default)"
    :default 2
    :parse-fn #(Integer/parseInt %)
    :validate [#(<= 0 % 2) "Precision must be between 0 and 2"]]
   ["-v" "--verbose" "Increase verbosity"]
   ["-h" "--help"]])

(defn usage [options-summary]
  (->> ["cdupes - find duplicate files in a directory"
        ""
        "Usage: cdupes [options] directory"
        ""
        "Options:"
        options-summary
        ""]
       (string/join \newline)))

(defn error-msg [errors]
  (str "The following errors occurred while parsing your command:\n\n"
       (string/join \newline errors)))

(defn exit [status msg]
  (println msg)
  (System/exit status))

(defn size-str-to-bytes [ size-str ]
  "Converts a string such as one of the following:

   50
   200B
   10K
   20M
   2G

   To the corresponding value in bytes."
  (when-let [match (re-matches #"(\d+)([b|k|m|g])?" (.toLowerCase size-str))]
    (let [size (Integer/parseInt (second match))]
      (case (second (rest match))
        nil size
        "b" size
        "k" (* 1024 size)
        "m" (* 1048576 size)
        "g" (* 1073741824 size)))))

;; hash map of <File> -> <Str>
;; <Str> representing the md5sum of the file
(def md5-hash-map {})
(def length-hash-map {})

;; Filters
(defn size-filter [ size-str ]
  "Takes a string which represents the size of a file,
it can be in the form of \"-500\" meaning 'less than 500 bytes',
\"+500\" meaning 'greater than 500 bytes', or just \"500\" meaning
exactly 500 bytes.

It returns a lambda comparing a clojure.java.io/file object's length
to the size."
  (let [ cmp (get {\- <, \+ >} (first size-str) =)
        constraint-in-bytes (if (or (= \- (first size-str))
                                    (= \+ (first size-str)))
                              (size-str-to-bytes (subs size-str 1))
                              (size-str-to-bytes size-str)) ]
    (fn [f]
      (cmp (.length f) constraint-in-bytes))))

(defn regex-filter [ regex-str negate ]
  "Takes a regular expression in the form of a string, such as \".*zsh.*\"
and a boolean as to whether or not to negate the result.

It returns a lambda which converts it to a java regex, and performs the match
on the file objects string from getName"
  (fn [f]
    (let [ re (re-matches (java.util.regex.Pattern/compile regex-str) (.getName f)) ]
      (if negate
        (not re)
        re))))

;; Determining if files are duplicates
(defn md5-file [file]
  "Returns the md5sum of a file.

If the md5sum doesn't exist in md5-hash-map it adds it to reuse later."
  (if (not (get md5-hash-map file))
    (def md5-hash-map (assoc md5-hash-map file (digest/md5 file))))
  (get md5-hash-map file))

(defn length-file [file]
  "Returns the length of a file in bytes."
  (if (not (get length-hash-map file))
    (def length-hash-map (assoc length-hash-map file (.length file))))
  (get length-hash-map file))

(defn identical-by-length [ & files ]
  (apply = (map #(length-file %) files)))

(defn identical-by-checksum [ & files ]
  (apply = (map #(md5-file %) files)))

;; following 2 functions provided by dbasch
(defn byte-seq
  [^java.io.BufferedReader rdr]
  (when-let [b (.read rdr)]
    (when (not= b -1)
      (cons b (lazy-seq (byte-seq rdr))))))

(defn identical-byte-by-byte [ & files ]
  (with-open [rdr1 (clojure.java.io/reader (first files))
              rdr2 (clojure.java.io/reader (second files))]
    (let [s1 (byte-seq rdr1)
          s2 (byte-seq rdr2)]
      (every? true? (map = s1 s2)))))

;; 500mb
(def ^:const large-file-threshold 524288000)

(defn partial-bytes [ file ]
  "Returns the first and last 5mb as a vector."
  (def byte-sequences (vec nil))
  (let [ raf (RandomAccessFile. (.getAbsolutePath file) "r")

        buf (byte-array 5242880) ]  ;; first 5mb
    (.read raf buf)
    (def byte-sequences (conj byte-sequences buf))
    (.seek raf (- (.length file) 5242880))
    (let [ buf2 (byte-array 5242880)] ;; last 5mb
      (.read raf buf2)
      (conj byte-sequences buf2))))

(defn identical-by-partial-checksum [ & files ]
  "Assumes files are both identical lengths,
and over large-file-threshold.

This is sort of a ridiculous edgecase, but if 2 large files are identical sizes,
we checksum the first 5mb and the last 5mb of each file and compare them."
  (= (map digest/md5 (partial-bytes (first files)))
     (map digest/md5 (partial-bytes (second files)))))

(defn identical-files [ file1 file2 precision ]
  "Considers a file is identical based on precision, see cli-options for precision
definition."
  (case precision
    0 (identical-by-length file1 file2)
    1 (and (identical-by-length file1 file2)
           (if (> (.length file1) large-file-threshold)
             (identical-by-partial-checksum file1 file2)
             true)
           (identical-by-checksum file1 file2))
    2 (and (identical-by-length file1 file2)
           (identical-byte-by-byte file1 file2))))
;; perhaps use every-pred here?
;; catch FileNotFoundException in case files no longer exist

;; File objects within a path
(defn eligible-files [ path recurse ]
  "Return only files within a given path, recursively or not."
  (filter #(.isFile %) (if recurse
                         (file-seq (file path))
                         (.listFiles (file path)))))

(defn duplicates-of-file [  file files precision ]
  "Given a file, and a list of potential matching files, and a precision level,
filter out the files that are identical and not the file itself."
  (filter (fn [f]
            (and (not= f file)
                 (identical-files f file precision))) files))

(defn --apply-options [ options arguments ]
  "Returns the eligible files based on cli-options."
  (filter
   (every-pred
    (if (:size options)
      (size-filter (:size options))
      (fn [& args] true))
    (if (:regex options)
      (regex-filter (:regex options) (:invert-regex options))
      (fn [& args] true)))

   (eligible-files (first arguments) (:recurse options))))


;; when-let

(defn -main
  "Handles command line arguments, prints error messages/help if needed.

  Then builds a list of eligible files based on the CLI options, and loops through each
  one finding all duplicates, and printing the group of identical files together."
  [& args]
  (let [{:keys [options arguments errors summary]} (parse-opts args cli-options)]
    (cond
      (:help options) (exit 0 (usage summary))
      (not= (count arguments) 1) (exit 1 (usage summary))
      errors (exit 1 (error-msg errors)))
    (printf "Building file list...")
    (flush)
    (let [ files (--apply-options options arguments) ] ;; eligible files
      (printf "done. (%d files to examine)\n" (count files))
      (flush)
      (reduce (fn [mentioned-files file]
                ;; if the file is already mentioned, just return mentioned-files
                (if (some #{file} mentioned-files)
                  mentioned-files
                  ;; otherwise, find duplicates
                  ;; print them, and return the duplicates concatenated with mentioned-files
                  (let [duplicates (duplicates-of-file file files (:precision options))]
                    (if (seq duplicates)
                      (printf "%s\n" file))
                    (doseq [dup duplicates]
                      (printf "%s\n" dup)
                      (flush))
                    (if (seq duplicates)
                      (println))
                    (concat mentioned-files duplicates)))) () files))))
