<template>
  <default-layout title="Planner">
    <div class="container mx-auto border-black border-2 rounded p-4">
      <div class="pb-2">Enter Plan:</div>
      <div class="flex items-stretch mb-4">
        <textarea class="border border-black grow rounded p-0.5"/>
      </div>
      <Button text="Send"></Button>
    </div>
  </default-layout>
</template>

<style>

</style>
<script setup lang="ts">
import DefaultLayout from '@/layout/DefaultLayout.vue'
import Button from '@/components/Button.vue'
</script>