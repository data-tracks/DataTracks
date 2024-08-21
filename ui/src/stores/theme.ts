import { computed, ref } from 'vue'
import { defineStore } from 'pinia'

export enum Theme {
  DARK = 'dark',
  LIGHT = 'light',
}

export const useThemeStore = defineStore('theme', () => {
  const hasDarkPreference = window.matchMedia(
    "(prefers-color-scheme: dark)"
  ).matches;
  const initialTheme = localStorage.getItem('theme') || hasDarkPreference ? Theme.DARK : Theme.LIGHT;
  const theme = ref(initialTheme)
  const isDark = computed(() => theme.value === Theme.DARK)

  function changeTheme(t: Theme) {
    theme.value = t;
    localStorage.setItem("theme", theme.value);
  }

  return { theme, isDark, changeTheme }
})
