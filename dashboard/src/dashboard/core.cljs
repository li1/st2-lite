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

(defn max-worker [pag-data]
  (->> pag-data
       (js->clj)
       (map #(get-in % ["src" "wid"]))
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

(def colors {"Waiting"    "#f44336"
             "Progress"   "#0277bd"
             "Spinning"   "#b0bec5"
             "Processing" "#00c853"
             "Busy"       "#000"
             "Data"       "#8e24aa"})

(defn mount-marker [defs color]
  (.. defs (append "marker")
      (attr "id" (first color))
      (attr "viewBox" "0 0 10 10")
      (attr "refX" 6)
      (attr "refY" 5)
      (attr "markerUnits" "strokeWidth")
      (attr "markerWidth" 4)
      (attr "markerHeight" 4)
      (attr "orient" "auto")
      (append "path")
      (attr "d" "M 0 0 L 10 5 L 0 10 z")
      (attr "fill" (second color))))

(defn mount-markers [svg]
  (let [defs (.. svg (append "defs"))
        markers (run! (partial mount-marker defs) colors)]
    defs))

(defn type-name [type]
  (let [type (js->clj type)]
    (if (string? type) type
        (first (keys type)))))

(defn proc-title [p]
  (str " (o" (get p "oid") ", " (+ (get p "send") (get p "recv")) ")"))

(defn gen-title [d]
  (let [type (js->clj (.-edge_type d))]
    (if (string? type)
      type
      (let [name (first (keys type))]
        (case name
          "Processing" (str name (proc-title (get type name)))
          "Spinning"   (str name " (o" (get type name) ")")
          (str name " (" (get type name) ")"))))))

(defn redraw-pag [pag-svg pag-data x-scale y-scale]
  (.. pag-svg
      (selectAll "line")
      (data pag-data)
      (join "line")
      (attr "stroke-width" 2)
      (attr "stroke" #(get colors (type-name (.-edge_type %))))
      (attr "stroke-dasharray" #(if (= (.. % -src -wid) (.. % -dst -wid)) "5,0" "5,5"))
      (attr "marker-end" #(str "url(#" (type-name (.-edge_type %)) ")"))
      (attr "x1" #(x-scale (dur->ns (.. % -src -t))))
      (attr "x2" #(x-scale (dur->ns (.. % -dst -t))))
      (attr "y1" #(y-scale (.. % -src -wid)))
      (attr "y2" #(y-scale (.. % -dst -wid))))
    (.. pag-svg (selectAll "text")
        (data pag-data)
        (join "text")
        (style "font-size" 12)
        (attr "x" #(- (x-scale (/ (+ (dur->ns (.. % -src -t)) (dur->ns (.. % -dst -t))) 2)) 30))
        (attr "y" #(- (y-scale (/ (+ (.. % -src -wid) (.. % -dst -wid)) 2)) 10))
        (attr "fill" #(get colors (type-name (.-edge_type %))))
        (text gen-title)))

(defn zoomed [x-axis-svg x-axis x-scale y-scale pag-svg pag-data]
  (let [new-x-scale (.. js/d3 -event -transform (rescaleX x-scale))]
    (.call x-axis-svg (.scale x-axis new-x-scale))
    (redraw-pag pag-svg pag-data new-x-scale y-scale)))

(defn mount-show-labels [pag-svg pag-data]
  (let [checkbox (.. js/document (getElementById "showlabel"))]
    (.addEventListener checkbox "change"
                       #(this-as this
                         (.. pag-svg
                             (selectAll "text")
                             (data pag-data)
                             (attr "opacity" (if (.-checked this) 1 0)))))))

(defn mount-d3 [pag-data]
  (let [width (js/parseInt (.. js/d3 (select "#pag") (style "width")))
        height (+ 80 (* 120 (max-worker pag-data)))
        svg (mount-svg width height)
        marker (mount-markers svg)
        x-scale (.. js/d3 (scaleLinear) (domain #js [(pag-min pag-data) (pag-max pag-data)]) (range #js [80 (- width 20)]))
        x-axis (.. js/d3 (axisBottom x-scale))
        x-axis-svg (.. svg (append "g") (call x-axis))
        y-scale (.. js/d3 (scaleLinear) (domain #js [0 (max-worker pag-data)]) (range #js [80 (- height 20)]))
        y-axis (.. js/d3 (axisLeft y-scale) (ticks 1))
        y-axis-svg (.. svg (append "g") (attr "transform" "translate(40, 0)") (call y-axis))
        pag-svg (.. svg (append "g"))]
    (.call svg (.. js/d3 (zoom) (on "zoom" #(zoomed x-axis-svg x-axis x-scale y-scale pag-svg pag-data))))
    (redraw-pag pag-svg pag-data x-scale y-scale)
    (mount-show-labels pag-svg pag-data)
    (.log js/console pag-data)))

(mount-d3 (.parse js/JSON data/json2))
