/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

var PlanVizConstants = { svgMarginX: 16, svgMarginY: 16 };

function shouldRenderPlanViz() {
  return planVizContainer().selectAll("svg").empty();
}

// SC-32974: In DBR, all the URLs from Spark UI are proxied.
// For example, there is such paths after UI root:
//   "sparkui/0508-033419-trust656/driver-4459564772855259754/"
// To render the URLs for stages correctly, it is required to update the URL in
// the dot file.
function updateUrlInDotFile() {
  var vizCss = $("#spark-sql-viz-css");
  if (vizCss.length) {
    var href = vizCss.attr("href");
    var pos = href.indexOf("/static");
    var prefix = href.substring(0, pos);
    var dotFile = $(".dot-file");
    var text = dotFile
      .text()
      .split("href=/stage")
      .join("href=" + prefix + "/stage");
    dotFile.text(text);
  }
}

function adjustLabelPositionInCluster() {
  $(".cluster").each(function () {
    var label = $(this).find(".label");
    var translateString = label.attr("transform");
    if (translateString.includes("translate(")) {
      var labelWidth = parseInt(label.find("foreignObject").css("width"));
      var leftParenthesisPos = translateString.indexOf("(");
      var rightParenthesisPos = translateString.indexOf(")");
      var commaPos = translateString.indexOf(",");
      // The HTML label is always rendered near the outer right side of the
      // cluster. We need to move it into the cluster.
      var x =
        translateString.substring(leftParenthesisPos + 1, commaPos) -
        labelWidth -
        20;
      var y = translateString.substring(commaPos + 1, rightParenthesisPos);
      var translate = "translate(" + x + "," + y + ")";
      label.attr("transform", translate);
    }
  });
}

function renderPlanViz() {
  var svg = planVizContainer().append("svg");
  var metadata = d3.select("#plan-viz-metadata");
  var dot = metadata.select(".dot-file").text().trim();
  var graph = svg.append("g");

  var g = graphlibDot.read(dot);
  preprocessGraphLayout(g);
  var renderer = new dagreD3.render();
  renderer(graph, g);

  // Round corners on rectangles
  svg.selectAll("rect").attr("rx", "5").attr("ry", "5");
  classifyPhotonNodesAndClusters(svg);
  adjustLabelPositionInCluster();
  resizeSvg(svg);
  setupDownloadButton();

  // Once the query plan graph is built, we need to update
  // the tables to make them sortable.
  $(".sql-metrics-table").each(function (id, val) {
    sorttable.makeSortable(val);
  });
}

/*
 * If the element represents portion of the query plan executed by the Photon
 * engine, add it to the provided class.
 */
function addClassIfPhotonElement(d3elem, classToAdd) {
  var name = d3elem.attr("name");
  var isPhotonElem = name && name.indexOf("Photon") >= 0;
  d3elem.classed(classToAdd, isPhotonElem);
}

/*
 * Find all clusters and nodes executed by the Photon engine, and classify them
 * as either photonCluster, or photonNode. This allows us to visually highlight
 * these elements in the graph.
 */
function classifyPhotonNodesAndClusters(svg) {
  // Process all immediate children of elements with class "g.nodes"
  svg.selectAll("g.nodes > *").each(function () {
    addClassIfPhotonElement(d3.select(this), "photonNode");
  });
  // Process all immediate children of elements with class "g.clusters"
  svg.selectAll("g.clusters > *").each(function () {
    addClassIfPhotonElement(d3.select(this), "photonCluster");
  });
}

/* -------------------- *
 * | Helper functions | *
 * -------------------- */

function planVizContainer() {
  return d3.select("#plan-viz-graph");
}

/*
 * Set up the tooltip for a SparkPlan node using metadata. When the user moves
 * the mouse on the node, it will display the details of this SparkPlan node in
 * the right.
 */
function setupTooltipForSparkPlanNode(nodeId) {
  var nodeTooltip = d3.select("#plan-meta-data-" + nodeId).text();
  d3.select("svg g .node_" + nodeId).each(function (d) {
    var domNode = d3.select(this).node();
    $(domNode).tooltip({
      title: nodeTooltip,
      trigger: "hover focus",
      container: "body",
      placement: "top",
    });
  });
}

/*
 * Helper function to pre-process the graph layout.
 * This step is necessary for certain styles that affect the positioning
 * and sizes of graph elements, e.g. padding, font style, shape.
 */
function preprocessGraphLayout(g) {
  var nodes = g.nodes();
  for (var i = 0; i < nodes.length; i++) {
    var node = g.node(nodes[i]);
    node.padding = "5";
  }
  // Curve the edges
  var edges = g.edges();
  for (var j = 0; j < edges.length; j++) {
    var edge = g.edge(edges[j]);
    edge.lineInterpolate = "basis";
  }
}

/*
 * Helper function to size the SVG appropriately such that all elements are
 * displayed. This assumes that all outermost elements are clusters
 * (rectangles).
 */
function resizeSvg(svg) {
  var allClusters = svg.selectAll("g rect")[0];
  var startX =
    -PlanVizConstants.svgMarginX +
    toFloat(
      d3.min(allClusters, function (e) {
        return getAbsolutePosition(d3.select(e)).x;
      })
    );
  var startY =
    -PlanVizConstants.svgMarginY +
    toFloat(
      d3.min(allClusters, function (e) {
        return getAbsolutePosition(d3.select(e)).y;
      })
    );
  var endX =
    PlanVizConstants.svgMarginX +
    toFloat(
      d3.max(allClusters, function (e) {
        var t = d3.select(e);
        return getAbsolutePosition(t).x + toFloat(t.attr("width"));
      })
    );
  var endY =
    PlanVizConstants.svgMarginY +
    toFloat(
      d3.max(allClusters, function (e) {
        var t = d3.select(e);
        return getAbsolutePosition(t).y + toFloat(t.attr("height"));
      })
    );
  var width = endX - startX;
  var height = endY - startY;
  svg
    .attr("viewBox", startX + " " + startY + " " + width + " " + height)
    .attr("width", width)
    .attr("height", height);
}

/* Helper function to convert attributes to numeric values. */
function toFloat(f) {
  if (f) {
    return parseFloat(f.toString().replace(/px$/, ""));
  } else {
    return f;
  }
}

/*
 * Helper function to compute the absolute position of the specified element in
 * our graph.
 */
function getAbsolutePosition(d3selection) {
  if (d3selection.empty()) {
    throw "Attempted to get absolute position of an empty selection.";
  }
  var obj = d3selection;
  var _x = toFloat(obj.attr("x")) || 0;
  var _y = toFloat(obj.attr("y")) || 0;
  while (!obj.empty()) {
    var transformText = obj.attr("transform");
    if (transformText) {
      var translate = d3.transform(transformText).translate;
      _x += toFloat(translate[0]);
      _y += toFloat(translate[1]);
    }
    // Climb upwards to find how our parents are translated
    obj = d3.select(obj.node().parentNode);
    // Stop when we've reached the graph container itself
    if (obj.node() === planVizContainer().node()) {
      break;
    }
  }
  return { x: _x, y: _y };
}

function reRenderPlanViz() {
  // Render the new SVG before removing the old one, so that we can avoid page
  // jumping.
  renderPlanViz();
  d3.selectAll(".tooltip").remove();
  d3.select("svg").remove();
  setupSQLPlanTooltip();
}

function setupDownloadButton() {
  d3.select("#saveButton").on("click", function () {
    setupInlineStyle(d3.select("svg"));
    html2canvas(document.body).then(function (canvas) {
      saveAs(canvas.toDataURL(), document.title + ".png");
    });
  });
}

// Save the given uri as filename.
// This is from https://stackoverflow.com/a/26361461.
function saveAs(uri, filename) {
  var link = document.createElement("a");
  if (typeof link.download === "string") {
    link.href = uri;
    link.download = filename;
    // Firefox requires the link to be in the body
    document.body.appendChild(link);
    // simulate click
    link.click();
    // remove the link when done
    document.body.removeChild(link);
  } else {
    window.open(uri);
  }
}

function clickPlanNodeDetails(id) {
  $("#plan-node-details-arrow-" + id)
    .toggleClass("arrow-open")
    .toggleClass("arrow-closed");
  $("#plan-node-details-" + id).toggle();
  let dotFile = $(".dot-file");
  let text = dotFile.text();
  let hide =
    "<div id='plan-node-details-" +
    id +
    "' style='display: none;' class='plan-details-search'>";
  let show = "<div id='plan-node-details-" + id + "'>";
  if (text.includes(hide)) {
    dotFile.text(text.replace(hide, show));
  } else {
    dotFile.text(text.replace(show, hide));
  }
  reRenderPlanViz();
}

function showHiddenMetrics() {
  let checkBox = document.getElementById("showSQLPlanHiddenMetricsCheckBox");
  let dotFile = $(".dot-file");
  let text = dotFile.text();
  if (checkBox.checked) {
    dotFile.text(
      text.replace(
        / style='display: none;' class='hideable-cell'/g,
        " class='hideable-cell'"
      )
    );
  } else {
    dotFile.text(
      text.replace(
        / class='hideable-cell'/g,
        " style='display: none;' class='hideable-cell'"
      )
    );
  }
  reRenderPlanViz();
}

function expandAll() {
  let checkBox = document.getElementById("expandSQLPlanDetailsCheckBox");
  let dotFile = $(".dot-file");
  let text = dotFile.text();
  if (checkBox.checked) {
    dotFile.text(
      text.replace(/ style='display: none;' class='plan-details-search'/g, "")
    );
  } else {
    dotFile.text(
      text.replace(
        /(id='plan-node-details-\d+')>/g,
        "$1 style='display: none;' class='plan-details-search'>"
      )
    );
  }
  reRenderPlanViz();
}

function setupSQLPlanTooltip() {
  var nodeSize = parseInt($("#plan-viz-metadata-size").text());
  for (var i = 0; i < nodeSize; i++) {
    setupTooltipForSparkPlanNode(i);
  }
}

function setupInlineStyle(svg) {
  // Here we have to compute and inline the styles of the nodes in the SVG.
  // Otherwise, the style are lost if we tried to download the screen as png.
  // See: https://github.com/niklasvh/html2canvas/issues/1123 and
  //      https://github.com/lukehorvat/computed-style-to-inline-style#why
  computedStyleToInlineStyle(svg.node(), {
    recursive: true,
    properties: [
      "font-size",
      "color",
      "fill",
      "stroke",
      "stroke-width",
      "background",
      "margin-top",
      "margin-bottom",
    ],
  });
}
