<script setup lang="ts">
import { RouterView } from 'vue-router'
import Header from '@/components/Header.vue'
import Footer from '@/components/Footer.vue'
import Toast from '@/components/Toast.vue'
import ModalService from '@/components/ModalService.vue'
import { useThemeStore } from '@/stores/theme'
import { storeToRefs } from 'pinia'

const themeStore = useThemeStore();

const {isDark} = storeToRefs(themeStore);

</script>

<template>
  <div class="flex h-full flex-col">
    <div class="content flex flex-row grow">
      <Header />
      <main class="grow flex flex-col" :class="{'bg-gray-700' : isDark}">
        <Toast />
        <ModalService />
        <router-view v-slot="{ Component }" class="grow">
          <transition name="fade" mode="out-in" :duration="{ enter: 300, leave: 100 }">
            <component :is="Component" />
          </transition>
        </router-view>
        <Footer />
      </main>
    </div>
  </div>
</template>

<style lang="scss">
body {
  height: 100vh;

  #app {
    height: 100%;
  }
}

.fade-enter-active,
.fade-leave-active {
  transition: opacity 0.3s ease;
}

.fade-enter-from,
.fade-leave-to {
  opacity: 0;
}

</style>
