<script setup lang="ts">
import DefaultLayout from '@/layout/DefaultLayout.vue'
import Button from '@/components/default/Button.vue'
import Form from '@/components/default/Form.vue'
import { ref } from 'vue'
import { usePlanStore } from '@/stores/plan'
import Card from '@/components/default/Card.vue'
import { useThemeStore } from '@/stores/theme'
import { storeToRefs } from 'pinia'

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

const themeStore = useThemeStore();

const {isDark} = storeToRefs(themeStore);

const FORM_STYLES = "border-0 rounded-sm p-1 shadow ring-1 ring-inset ring-gray-300 focus:ring-2 placeholder:text-gray-500"
</script>

<template>
  <default-layout title="Planner">
    <Card :has-padding="true">
      <div>
        <div>Name</div>
        <Form :is-valid="name !== ''">
          <input type="text" :class="FORM_STYLES" v-model="name" placeholder="Enter name...">
        </Form>
      </div>

      <div class="flex items-stretch flex-col mt-2">
        <div>Schedule</div>
        <Form :is-valid="schedule !== ''">
          <textarea :class="FORM_STYLES" v-model="schedule" placeholder="Enter schedule..." />
        </Form>
      </div>

      <div class="mt-3">
        <Button text="Send" @click="submit" :disabled="schedule === '' || name === ''"></Button>
      </div>

    </Card>
  </default-layout>
</template>

<style>

</style>
