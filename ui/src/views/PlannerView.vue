<script setup lang="ts">
import DefaultLayout from '@/layout/DefaultLayout.vue'
import Button from '@/components/default/Button.vue'
import Form from '@/components/default/Form.vue'
import { ref } from 'vue'
import { usePlanStore } from '@/stores/plan'

const schedule = ref('')
const name = ref('')

const store = usePlanStore()

const reset = () => {
  schedule.value = ''
  name.value = ''
}

const submit = async () => {
  await store.submitPlan(name.value, schedule.value)
  reset()
}
</script>

<template>
  <default-layout title="Planner">
    <div class="container mx-auto border-black border-2 rounded p-4 flex flex-col gap-2">
      <div>
        <div>Name</div>
        <Form :is-valid="name !== ''">
          <input type="text" v-model="name" placeholder="Enter name...">
        </Form>
      </div>

      <div class="flex items-stretch flex-col">
        <div>Schedule</div>
        <Form :is-valid="schedule !== ''">
          <textarea v-model="schedule" placeholder="Enter schedule..." />
        </Form>
      </div>

      <div class="mt-3">
        <Button text="Send" @click="submit" :disabled="schedule === '' || name === ''"></Button>
      </div>

    </div>
  </default-layout>
</template>

<style>

</style>
