<script setup lang="ts">
import DefaultLayout from '@/layout/DefaultLayout.vue'
import Plan from '@/components/Plan.vue'
import Card from '@/components/default/Card.vue'
import { getStop, usePlanStore } from '@/stores/plan'
import { onMounted } from 'vue'
import { storeToRefs } from 'pinia'
import { useThemeStore } from '@/stores/theme'
import Button from '@/components/default/Button.vue'
import Stop from '@/components/Stop.vue'

let store = usePlanStore()

const { plans } = storeToRefs(store)

let theme = useThemeStore()

const { isDark } = useThemeStore()

onMounted(async () => {
  await store.fetchPlans()
})

</script>

<template>
  <default-layout title="Plans">
    <div class="pb-6" v-for="network in plans" :key="network.name">
      <Card>
        <template v-slot:left>
          {{ network.name }}
        </template>
        <template v-slot:right>
          <div class="text-green-500">running</div>
        </template>

        <Plan :network="network" />

        <template v-slot:bottom v-if="store.currentNumber || store.currentNumber === 0">
          <div class="px-3 flex flex-col">
            <div>
              <Stop :stop="getStop(network, store.currentNumber)"></Stop>
            </div>
          </div>
        </template>
      </Card>
    </div>

  </default-layout>
</template>
