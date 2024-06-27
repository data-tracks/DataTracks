<script setup lang="ts">
import { RouterView } from 'vue-router'
import Header from '@/components/Header.vue'
import Footer from '@/components/Footer.vue'
import Toast from '@/components/Toast.vue'
import ModalService from '@/components/ModalService.vue'

</script>

<template>
  <div class="flex h-full flex-col">
    <Header />
    <main class="grow">
      <Toast />
      <ModalService />
      <RouterView />
    </main>
    <Footer />
  </div>
</template>

<style lang="scss">
body {
  height: 100vh;

  #app {
    height: 100%;
  }
}
</style>
