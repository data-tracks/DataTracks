<script setup lang="ts">
export interface IconProps {
  size?: string,
  fill?: string,
}

withDefaults(defineProps<IconProps>(), {
  size: '1.5rem',
  fill: 'black'
});
</script>

<template>
  <svg :width="size" :height="size" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
    <path d="M6 4H18M4 11H20M3 18L21 18M9.0002 3.00005L4.00009 21M20 21L15 3" :stroke="fill" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/>
  </svg>
</template>

<style scoped>

</style>