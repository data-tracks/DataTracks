<script setup lang="ts">
import * as d3 from 'd3'
import {computed, onMounted, ref, watchEffect} from 'vue'
import {type Link, type Node, type Plan, type Stop, usePlanStore} from '@/stores/plan'
import {v4} from 'uuid'
import {useModalStore} from '@/stores/modal'
import {useThemeStore} from '@/stores/theme'
import {storeToRefs} from 'pinia'

const X_GAP = 100
const Y_GAP = 60
const WIDTH = 40
const PADDING = 10
const TOTAL = WIDTH / 2 + PADDING

const id: string = v4()

const props = defineProps<{
  plan: Plan
}>()

const isRendered = ref(false)
const isMounted = ref(false)

const modal = useModalStore()

const themeStore = useThemeStore()
const planStore = usePlanStore()

const { theme, isDark } = storeToRefs(themeStore)
const { currentNumbers } = storeToRefs(planStore)

const lineColor = computed(() => isDark.value ? 'white' : 'black')
const tooltipBg = computed(() => isDark.value ? 'white' : 'grey')
const tooltipText = computed(() => isDark.value ? 'black' : 'white')

const extractNodes = (network: Plan): Node[] => {
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

const extractLinks = (network: Plan, nodes: Node[]): Link[] => {
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

const getStop = (node: Node): Stop | undefined => {
  return props.plan.stops.get(node.num)
}

function renderLines(svg: d3.Selection<SVGGElement, unknown, HTMLElement, any>, links: Link[]): void {
  // connections lines
  svg
    .append('g')
    .attr('stroke', 'black')
    .attr('stroke-opacity', 0.6)
    .selectAll()
    .data(links)
    .join('line')
    .attr('x1', (d) => d.source.x)
    .attr('y1', (d) => d.source.y)
    .attr('x2', (d) => d.target.x)
    .attr('y2', (d) => d.target.y)
    .attr('stroke-width', 5)
}

function renderNodesAndTooltip(svg: d3.Selection<SVGGElement, unknown, HTMLElement, any>, nodes: Node[]) {
  // create a tooltip
  const Tooltip = d3
    .select('.editor-wrapper-' + id)
    .append('div')
    .style('opacity', 0)
    .attr('class', 'tooltip border shadow-md')


  // Three function that change the tooltip when user hover / move / leave a cell
  const mouseover = (e: MouseEvent, d: Node) => {
    const el = props.plan.stops.get(d.num)
    let content = `<p>Stop: ${d.num}</p>`
    if (el?.transform) {
      console.log(el.transform.name)
      content += el.transform.display()
    }
    Tooltip.html(content).style('opacity', 1)
    const target = e.currentTarget as HTMLElement
    target.classList.add('hover')
  }
  const mousemove = (e: MouseEvent, d: Node) => {
    Tooltip
      .style('left', d.x + WIDTH + PADDING + 'px')
      .style('top', d.y + WIDTH + PADDING + 'px'
      )
  }
  const mouseleave = (e: MouseEvent, d: Node) => {
    Tooltip.style('opacity', 0)
    const target = e.currentTarget as HTMLElement
    target.classList.remove('hover')
  }


  // nodes
  svg
    .append('g')
    .attr('stroke', '#fff')
    .attr('stroke-width', 1.5)
    .selectAll()
    .data(nodes)
    .join('rect')
    .attr('x', (d) => d.x - WIDTH / 2)
    .attr('y', (d) => d.y - WIDTH / 2)
    .attr('width', WIDTH)
    .attr('height', WIDTH)
    .attr('rx', 4)
    .attr('class', (d) => 'station ' + color(d))
    .on('mouseover', mouseover)
    .on('mousemove', mousemove)
    .on('mouseleave', mouseleave)
    .on('click', (e, n) => {
      if (planStore.currentNumbers.get(props.plan.id) === n.num) {
        planStore.setCurrent(props.plan.id, null)
      } else {
        planStore.setCurrent(props.plan.id, n.num)
      }
    })
}

const color = (d: any) => {
  const stop = props.plan.stops.get(d.num)
  if (stop && stop.transform) {
    return 'trans'
  }
  return 'default'
}

function isEmpty(elements: any[] | undefined) {
  if (!elements) {
    return true
  }
  return elements.length == 0
}

function renderInputs(svg: d3.Selection<SVGGElement, unknown, HTMLElement, any>, nodes: Node[]) {
  // inputs
    svg.append('g')
        .attr("class", "inputs")
      .attr('stroke', '#fff')
      .attr('stroke-width', 1.5)
      .selectAll()
      .data(nodes.filter(n => !isEmpty(getStop(n)?.sources)))
        .join('circle')
      .attr('cx', (d) => d.x - WIDTH/2)
      .attr('cy', (d) => d.y + 2)
      .attr('r', 10)
      .classed('stop', true)
        .attr('fill', 'black');


}

function renderOutputs(svg: d3.Selection<SVGGElement, unknown, HTMLElement, any>, nodes: Node[]) {
  // outputs
  svg.append('g')
    .attr('stroke', '#fff')
    .attr('stroke-width', 1.5)
    .selectAll()
    .data(nodes.filter(n => !isEmpty(getStop(n)?.destinations)))
    .join('circle')
    .attr('cx', (d) => d.x + WIDTH / 2)
    .attr('cy', (d) => d.y + 2)
    .attr('r', 10)
    .classed('stop', true)
    .attr('fill', 'red')
}

function renderNumbers(svg: d3.Selection<SVGGElement, unknown, HTMLElement, any>, nodes: Node[]) {
  // stop number
  svg
    .append('g')
    .attr('class', 'labels')
    .selectAll('text')
    .data(nodes)
    .enter()
    .append('text')
    .attr('class', 'num')
    .attr('dx', (d) => d.x)
    .attr('dy', (d) => d.y)
    .style('text-anchor', 'middle')
    .text((d) => {
      return d.num
    })
}

const render = () => {
  // we have to wait that the component is mounted and that the data is actually loaded
  if (!isMounted.value || !props.plan || isRendered.value) {
    return
  }
  isRendered.value = true

  const nodes = extractNodes(props.plan)
  const links = extractLinks(props.plan, nodes)

  let initialSvg = d3
    .select('.editor-' + id)
    .append('div')
    .attr('class', 'editor-wrapper-' + id)
    .classed('editor-wrapper', true)
    .append('svg')
    .attr('preserveAspectRatio', 'xMinYMin meet')
    .attr('width', 200)
    .attr('height', 200)

  let svg = initialSvg
    .append('g')
    .classed('elements_wrapper', true)
    // shift so all nodes ar in the visible area
    .attr('transform', `translate(${TOTAL},${TOTAL})`)


  renderLines(svg, links)
  renderNodesAndTooltip(svg, nodes)
  renderInputs(svg, nodes)
  renderOutputs(svg, nodes)
  renderNumbers(svg, nodes)


  scale(initialSvg)
}

const scale = (svg: d3.Selection<SVGSVGElement, unknown, HTMLElement, any>) => {
  let box = svg.node()?.getBBox()
  if (!box) {
    return
  }
  svg.attr('height', box.height + PADDING)
  svg.attr('width', box.width + PADDING)
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
  <div :class="'editor-'+id" class="editor flex justify-center my-8"></div>
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
  background-color: v-bind('tooltipBg');
  color: v-bind('tooltipText');
  padding: .5rem;
  border-radius: .3rem;
  white-space: nowrap;
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

.station {
  cursor: pointer;

  &.hover {
    stroke: $moonstone;
  }
}

circle {
  cursor: pointer;
}

line {
  stroke: v-bind('lineColor');
}
</style>
