<script setup lang="ts">

import { useThemeStore } from '@/stores/theme'
import { storeToRefs } from 'pinia'
import { Status, useMetaStore } from '@/stores/meta'
import { ArrowDownBoldIcon, ArrowUpBoldIcon } from 'mdi-vue3'

const themeStore = useThemeStore()

const { isDark } = storeToRefs(themeStore)

const metaStore = useMetaStore();

const {connectionStatus} = storeToRefs(metaStore);

</script>

<template>
<div>
  <div class="text-green-400 flex gap-2" v-if="connectionStatus === Status.Connected">
    <ArrowUpBoldIcon width="1rem" class="fill-green-400"  />
    {{connectionStatus}}
  </div>
  <div class="text-red-400 flex gap-2" v-else>
    <ArrowDownBoldIcon width="1rem" class="fill-red-400"  />
    {{connectionStatus}}
  </div>
</div>
</template>

<style scoped>

</style>
