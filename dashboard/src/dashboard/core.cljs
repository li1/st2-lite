(ns ^:figwheel-hooks dashboard.core
  (:require [dashboard.data :as data])
  (:require-macros
   [dashboard.core :refer [html]]))

(defn dur->ns [dur]
  (let [dur (js->clj dur)]
    (+ (* 1000000000 (dur "secs"))
       (dur "nanos"))))

(defn pag-min [pag-data]
  (->> pag-data
       (js->clj)
       (map #(dur->ns (get-in % ["src" "t"])))
       (reduce #(if (< %1 %2) %1 %2))))

(defn pag-max [pag-data]
  (->> pag-data
       (js->clj)
       (map #(dur->ns (get-in % ["dst" "t"])))
       (reduce #(if (> %1 %2) %1 %2))))

;; (defn filter-pag [pag-data [from to]]
;;   (->> pag-data
;;        (js->clj)
;;        (filter #(and (>= (dur->ns (get-in % ["dst" "t"])) from)
;;                      (<= (dur->ns (get-in % ["src" "t"])) to)))
;;        (map #(do (.log js/console (count %)) %))
;;        (clj->js)))

(defn mount-svg [width height]
  (.. js/d3 (select "#pag") (append "svg")
      (attr "width" (- width 20))
      (attr "height" height)))

(defn stroke-colors [type]
  (let [k (if (string? type) type
              (first (keys type)))]
    (get {"Waiting"    "#FF0000"
          "Progress"   "#4b5f53"
          "Spinning"   "#e48282"
          "Processing" "#0b6623"
          "Data"       "#971757"} k)))

(defn proc-title [p]
  (str " (o" (get p "oid") ", " (+ (get p "send") (get p "recv")) ")"))

(defn gen-title [d]
  (let [type (js->clj (.-edge_type d))]
    (if (string? type)
      type
      (let [name (first (keys type))]
        (case name
          "Processing" (str name (proc-title (get type name)))
          (str name " (" (get type name) ")"))))))

(defn redraw-pag [pag-svg pag-data scale]
  (.. pag-svg
      (attr "transform" "translate(0, 40)")
      (selectAll "line")
      (data pag-data)
      (join "line")
      (attr "stroke-width" 2)
      (attr "stroke" #(stroke-colors (js->clj (.-edge_type %))))
      (attr "stroke-dasharray" #(if (= (.. % -src -wid) (.. % -dst -wid)) "5,0" "5,5"))
      (attr "x1" #(scale (dur->ns (.. % -src -t))))
      (attr "x2" #(scale (dur->ns (.. % -dst -t))))
      (attr "y1" #(+ 50 (* 50 (.. % -src -wid))))
      (attr "y2" #(+ 50 (* 50 (.. % -dst -wid)))))
  (.. pag-svg (selectAll "text")
      (data pag-data)
      (join "text")
      (style "font-size" 12)
      (attr "x" #(scale (- (/ (+ (dur->ns (.. % -src -t)) (dur->ns (.. % -dst -t))) 2) 20)))
      (attr "y" #(- (* 50 (/ (+ (inc (.. % -src -wid)) (inc (.. % -dst -wid))) 2)) 10))
      (attr "fill" #(stroke-colors (js->clj (.-edge_type %))))
      (text gen-title)))

(defn zoomed [x-axis-svg x-axis x-scale pag-svg pag-data]
  (let [new-x-scale (.. js/d3 -event -transform (rescaleX x-scale))]
    (.call x-axis-svg (.scale x-axis new-x-scale))
    (redraw-pag pag-svg pag-data new-x-scale)))

(defn mount-d3 [pag-data]
  (let [width (js/parseInt (.. js/d3 (select "#pag") (style "width")))
        height 600
        svg (mount-svg width height)
        x-scale (.. js/d3 (scaleLinear) (domain #js [(pag-min pag-data) (pag-max pag-data)]) (range #js [0 (- width 20)]))
        x-axis (.. js/d3 (axisBottom x-scale))
        x-axis-svg (.. svg (append "g") (call x-axis))
        pag-svg (.. svg (append "g"))]
    (.call svg (.. js/d3 (zoom) (on "zoom" #(zoomed x-axis-svg x-axis x-scale pag-svg pag-data))))
    (redraw-pag pag-svg pag-data x-scale)
    (.log js/console pag-data)))

(mount-d3 (.parse js/JSON data/json))
