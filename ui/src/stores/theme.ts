import { computed, ref } from 'vue'
import { defineStore } from 'pinia'

export enum Theme {
  DARK = 'dark',
  LIGHT = 'light',
}



export const useThemeStore = defineStore('theme', () => {
  const hasDarkPreference = window.matchMedia(
    '(prefers-color-scheme: dark)'
  ).matches

  const getTheme = (): Theme  => {
    const storage = localStorage.getItem('theme')?.toLowerCase()
    switch (storage) {
      case 'dark':
        return Theme.DARK
      case 'light':
        return Theme.LIGHT
      default:
        return hasDarkPreference ? Theme.DARK : Theme.LIGHT
    }
  }

  const initialTheme = getTheme()
  const theme = ref(initialTheme)
  const isDark = computed(() => theme.value === Theme.DARK)

  const changeTheme = (t: Theme) => {
    theme.value = t
    localStorage.setItem('theme', theme.value)
  }

  return { theme, isDark, changeTheme }
})
