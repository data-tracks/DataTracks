<script setup lang="ts">
import DefaultLayout from '@/layout/DefaultLayout.vue'
import Plan from '@/components/Plan.vue'
import Card from '@/components/default/Card.vue'
import { usePlanStore } from '@/stores/plan'
import { onMounted } from 'vue'
import { storeToRefs } from 'pinia'

let store = usePlanStore()

const {plans} = storeToRefs(store)

onMounted(async () => {
  await store.fetchPlans()
})

</script>

<template>
  <default-layout title="Monitoring">
    <div v-for="network in plans" :key="network.name">
      <Card>
        <template v-slot:left>
          {{network.name}}
        </template>
        <template v-slot:right>
          <div class="text-green-500">running</div>
        </template>

        <Plan :network="network" />
      </Card>
    </div>

  </default-layout>
</template>
