<script setup lang="ts">
import { ConfigModel, type Destination, InOut, type Source, type Stop, usePlanStore } from '@/stores/plan'
import Button from '@/components/default/Button.vue'
import { useModalStore } from '@/stores/modal'
import { useThemeStore } from '@/stores/theme'
import { storeToRefs } from 'pinia'
import Config from '@/components/Config.vue'
import Adder from '@/components/Adder.vue'
import { Addable, useOptionsStore } from '@/stores/options'
import InOutConfiguration from '@/components/InOutConfiguration.vue'
import { markRaw } from 'vue'


const props = defineProps<{
  planId: number
  stop: Stop | undefined
}>()

const modal = useModalStore()

const planStore = usePlanStore();

const addSource = (typeName: string, configs: ConfigModel[]) => {
  planStore.addInOut(props.planId, props.stop?.num || 0, typeName,InOut.Source, configs)
}

const addDestination = (typeName: string, configs: ConfigModel[]) => {
  planStore.addInOut(props.planId, props.stop?.num || 0, typeName, InOut.Destination, configs)
}

const into = (insOuts: Source[] | Destination[], onAdd: (typeName: string, c: ConfigModel[]) => void ) => {
  return insOuts.map(e => new Addable(e.type_name, e.configs, onAdd))
}

const openAddSource = () => {
  modal.openModal(markRaw(Adder), { adds: into(sources.value, addSource) })
}

const openAddDestination = () => {
  modal.openModal(markRaw(Adder), { adds: into(destinations.value, addDestination) })
}

const themeStore = useThemeStore()

const { isDark } = storeToRefs(themeStore)

const optionsStore = useOptionsStore()

const { sources, destinations } = storeToRefs(optionsStore)

optionsStore.fetchInOutOptions()

const openSourceDestination = (sourceDestination: Source | Destination) => {
  modal.openModal(InOutConfiguration, { inOut: sourceDestination })
}

</script>

<template>
  <div class="flex gap-2 min-h-14 items-center">
    <div class="sources  flex flex-col self-end items-center">
      <div v-for="source in stop?.sources" :key="source.id" class="p-4">
        <div @click="openSourceDestination(source)">{{ source.type_name }}</div>
      </div>
      <Button text="+ Source" @click="openAddSource()"></Button>
    </div>
    <div class="configuration grow border border-y-0 px-4 self-stretch flex items-center justify-center">
      <table class="table-fixed">
        <tbody>
        <tr>
          <td>Stop:</td>
          <td>{{ stop?.num }}</td>
        </tr>
        <template v-if="stop?.transform?.configs">
          <tr :key="key" v-for="[key, config] in stop?.transform?.configs">
            <Config :key_="key" :config="config"></Config>
          </tr>
        </template>
        </tbody>
      </table>
    </div>
    <div class="destinations flex flex-col self-end items-center">
      <div v-for="destination in stop?.destinations" :key="destination.id" class="p-4">
        <p>{{ destination.type_name }}</p>
      </div>
      <Button text="+ Destination" @click="openAddDestination()"></Button>
    </div>
  </div>
</template>

<style scoped>

</style>
