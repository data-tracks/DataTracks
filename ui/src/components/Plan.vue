<script setup lang="ts">
import * as d3 from 'd3'
import { onMounted, ref, watchEffect } from 'vue'
import { type Link, type Network, type Node } from '@/stores/plan'
import { v4 } from 'uuid'

const X_GAP = 100
const Y_GAP = 60
const RADIUS = 20

const id: string = v4()

const props = defineProps<{
  network: Network
}>()

const isRendered = ref(false)
const isMounted = ref(false)

const extractNodes = (network: Network): Node[] => {
  const nodes = []
  const used: number[] = []

  for (const [num, line] of network.lines) {
    let x = 0
    for (const stop of line.stops) {
      if (used.includes(stop)) {
        continue
      }

      nodes.push({ num: stop, x: X_GAP * x++, y: Y_GAP * num })
      used.push(stop)
    }
  }
  return nodes
}

const extractLinks = (network: Network, nodes: Node[]): Link[] => {
  const links = []

  for (const [num, line] of network.lines) {
    for (let i = 0; i < line.stops.length - 1; i++) {
      const source = nodes.find((n) => n.num == line.stops[i])
      const target = nodes.find((n) => n.num == line.stops[i + 1])

      if (!source || !target) {
        continue
      }

      links.push({ source: source, target: target })
    }
  }
  return links
}

const render = () => {
  // we have to wait that the component is mounted and that the data is actually loaded
  if (!isMounted.value || !props.network || isRendered.value) {
    return
  }
  isRendered.value = true

  const nodes = extractNodes(props.network)
  const links = extractLinks(props.network, nodes)

  const color = (d: any) => {
    const stop = props.network.stops.get(d.num)
    if (stop && stop.transform) {
      return 'trans'
    }
    return 'default'
  }

  let svg = d3
    .select('.editor-' + id)
    .append('div')
    .attr('class', 'editor-wrapper-' + id)
    .classed("editor-wrapper", true)
    .append('svg')
    .attr('preserveAspectRatio', 'xMinYMin meet')
    .attr('width', 200)
    .attr('height', 200)

  // Three function that change the tooltip when user hover / move / leave a cell
  const mouseover = (e: MouseEvent, d: Node) => {
    const el = props.network.stops.get(d.num)
    let content = '<p>Stop: ' + d.num + '</p>'
    if (el?.transform) {
      content +=
        '<p>Transform: ' +
        el.transform.language +
        '</p>\n<p>' +
        el.transform.query +
        '</p>'
    }
    Tooltip.html(content).style('opacity', 1)
    const target = e.currentTarget as HTMLElement
    d3.select(target).style('opacity', 0.8)
  }
  const mousemove = (e: MouseEvent, d: Node) => {
    Tooltip.style('left', d.x + 2 * RADIUS + 'px').style(
      'top',
      d.y + 2 * RADIUS + 'px'
    )
  }
  const mouseleave = (e: MouseEvent, d: Node) => {
    Tooltip.style('opacity', 0)
    const target = e.currentTarget as HTMLElement
    d3.select(target).style('opacity', 1)
  }

  // connections lines
  svg
    .append('g')
    .attr('stroke', 'black')
    .attr('stroke-opacity', 0.6)
    .selectAll()
    .data(links)
    .join('line')
    .attr('x1', (d) => d.source.x + RADIUS)
    .attr('y1', (d) => d.source.y + RADIUS)
    .attr('x2', (d) => d.target.x + RADIUS)
    .attr('y2', (d) => d.target.y + RADIUS)
    .attr('stroke-width', 5)

  // nodes
  svg
    .append('g')
    .attr('stroke', '#fff')
    .attr('stroke-width', 1.5)
    .selectAll()
    .data(nodes)
    .join('circle')
    .attr('cx', (d) => d.x + RADIUS)
    .attr('cy', (d) => d.y + RADIUS)
    .attr('r', RADIUS)
    .attr('class', (d) => color(d))
    .on('mouseover', mouseover)
    .on('mousemove', mousemove)
    .on('mouseleave', mouseleave)

  // stop number
  svg
    .append('g')
    .attr('class', 'labels')
    .selectAll('text')
    .data(nodes)
    .enter()
    .append('text')
    .attr('class', 'num')
    .attr('dx', (d) => d.x + RADIUS)
    .attr('dy', (d) => d.y + RADIUS)
    .style('text-anchor', 'middle')
    .text((d) => {
      return d.num
    })

  // create a tooltip
  const Tooltip = d3
    .select('.editor-wrapper-'+id)
    .append('div')
    .style('opacity', 0)
    .attr('class', 'tooltip')
    .style('background-color', 'white')
    .style('border', 'solid')
    .style('border-width', '2px')
    .style('border-radius', '5px')
    .style('padding', '5px')

  scale(svg)
}

const scale = (svg: d3.Selection<SVGSVGElement, unknown, HTMLElement, any>) => {
  let box = svg.node()?.getBBox()
  if (!box) {
    return
  }
  svg.attr('height', box.height)
  svg.attr('width', box.width)
}

onMounted(() => {
  isMounted.value = true
  render()
})
watchEffect(() => {
  render()
})

</script>

<template>
  <div :class="'editor-'+id" class="editor flex justify-center"></div>
</template>

<style lang="scss">
//https://coolors.co/d8dbe2-a9bcd0-58a4b0-373f51-daa49a
/* SCSS HEX */
$platinum: #d8dbe2ff;
$powder-blue: #a9bcd0ff;
$moonstone: #58a4b0ff;
$charcoal: #373f51ff;
$melon: #daa49aff;


text.num {
  transform: translateY(.3rem)
}

.labels {
  pointer-events: none;
}

.editor {
  position: relative;
}

.editor-wrapper {
  position: relative;
}

.tooltip {
  z-index: 2;
  position: absolute;
}

p {
  margin: 0;
}

.default {
  fill: $moonstone;
}

.trans {
  fill: $melon;
}

circle {
  cursor: pointer;
}
</style>