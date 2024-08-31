<script setup lang="ts">
import DefaultLayout from '@/layout/DefaultLayout.vue'
import Plan from '@/components/Plan.vue'
import Card from '@/components/default/Card.vue'
import { getStop, PlanStatus, usePlanStore } from '@/stores/plan'
import { onMounted } from 'vue'
import { storeToRefs } from 'pinia'
import { useThemeStore } from '@/stores/theme'
import Stop from '@/components/Stop.vue'
import { PauseBoxIcon, PlayBoxIcon } from 'mdi-vue3'

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
    <div class="pb-6" v-for="plan in plans" :key="plan.name">
      <Card>
        <template v-slot:left>
          {{ plan.name }}
        </template>
        <template v-slot:right>
          <div class="flex items-center">
            <div
              :class="{'text-gray-500' : plan.status === PlanStatus.Stopped, 'text-green-500': plan.status === PlanStatus.Running, 'text-red-500': plan.status === PlanStatus.Error }">
              {{ plan.status }}...
            </div>
            <PlayBoxIcon class="fill-blue-500 cursor-pointer hover:fill-blue-300" width="2rem"
                         v-if="plan.status === PlanStatus.Stopped" @click="store.startPlan(plan.id)" />
            <PauseBoxIcon class="fill-blue-500 cursor-pointer hover:fill-blue-300" width="2rem"
                          v-if="plan.status === PlanStatus.Running" @click="store.stopPlan(plan.id)" />
          </div>
        </template>

        <Plan :plan="plan" />

        <template v-slot:bottom v-if="store.currentNumbers.get(plan.id) || store.currentNumbers.get(plan.id) === 0">
          <div class="px-3 flex flex-col">
            <div>
              <Stop :stop="getStop(plan, store.currentNumbers.get(plan.id) || 0)" :plan-id="plan.id"></Stop>
            </div>
          </div>
        </template>
      </Card>
    </div>

  </default-layout>
</template>
