<script setup lang="ts">
import * as d3 from 'd3'
import { computed, onMounted, watchEffect } from 'vue'
import { type Network, usePlanStore } from '@/stores/plan'

const X_GAP = 100
const Y_GAP = 60
const RADIUS = 20

let props = defineProps<{
  network: Network
}>()



class Node {
  num: number
  x: number
  y: number

  constructor(num: number, x: number, y: number) {
    this.num = num
    this.x = x
    this.y = y
  }
}

class Link {
  source: Node
  target: Node

  constructor(source: Node, target: Node) {
    this.source = source
    this.target = target
  }
}


const extractNodes = (network: Network): Node[] => {
  const nodes = []
  const used: number[] = []

  for (const [num, line] of network.lines) {
    let x = 0
    for (const stop of line.stops) {
      if (used.includes(stop)) {
        continue
      }

      nodes.push(new Node(stop, X_GAP * x++, Y_GAP * num))
      used.push(stop)
    }
  }
  return nodes
}

const extractLinks = (network: Network, nodes: Node[]): Link[] =>  {
  const links = []

  for (const [num, line] of network.lines) {
    for (let i = 0; i < line.stops.length - 1; i++) {
      const source = nodes.find((n) => n.num == line.stops[i])
      const target = nodes.find((n) => n.num == line.stops[i + 1])

      if (!source || !target) {
        continue
      }

      links.push(new Link(source, target))
    }
  }
  return links
}
watchEffect(() => {
  console.log(props.network)
  const nodes = extractNodes(props.network)
  const links = extractLinks(props.network, nodes)

  console.log(nodes)
  console.log(links)

  const color = (d: any) => {
    const stop = props.network.stops.get(d.num)
    if (stop && stop.transform) {
      return 'trans'
    }
    return 'default'
  }

  const svg = d3
    .select('.editor')
    .append('div')
    .attr('class', 'editor-wrapper')
    .append('svg')
    //.attr('width', 600)
    //.attr('height', 600)

  // Three function that change the tooltip when user hover / move / leave a cell
  const mouseover = ( e: MouseEvent, d: Node) => {
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
    const target = e.currentTarget as HTMLElement;
    d3.select(target).style('opacity', 0.8)
  }
  const mousemove = (e: MouseEvent, d: Node) => {
    Tooltip.style('left', d.x + 2 * RADIUS + 'px').style(
      'top',
      d.y + 2 * RADIUS + 'px'
    )
  }
  const mouseleave = (e:MouseEvent, d:Node) => {
    Tooltip.style('opacity', 0)
    const target = e.currentTarget as HTMLElement;
    d3.select(target).style('opacity', 1)
  }

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
    .select('.editor-wrapper')
    .append('div')
    .style('opacity', 0)
    .attr('class', 'tooltip')
    .style('background-color', 'white')
    .style('border', 'solid')
    .style('border-width', '2px')
    .style('border-radius', '5px')
    .style('padding', '5px')


})

</script>

<template>
<div class="editor flex justify-center"></div>
</template>

<style lang="scss">
//https://coolors.co/d8dbe2-a9bcd0-58a4b0-373f51-daa49a
/* SCSS HEX */
$platinum: #d8dbe2ff;
$powder-blue: #a9bcd0ff;
$moonstone: #58a4b0ff;
$charcoal: #373f51ff;
$melon: #daa49aff;


text.num{
  transform: translateY(.3rem)
}

.labels{
  pointer-events: none;
}

.editor{
  position: relative;
}
.editor-wrapper{
  position: relative;
}

.tooltip{
  z-index: 2;
  position: absolute;
}

p {
  margin: 0;
}

.default {
  fill: $moonstone;
}

.trans{
  fill: $melon;
}

circle{
  cursor: pointer;
}
</style>