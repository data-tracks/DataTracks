interface Theme {
  "color-scheme": string
  "--color-base-100": string
  "--color-base-200": string
  "--color-base-300": string
  "--color-base-content": string
  "--color-primary": string
  "--color-primary-content": string
  "--color-secondary": string
  "--color-secondary-content": string
  "--color-accent": string
  "--color-accent-content": string
  "--color-neutral": string
  "--color-neutral-content": string
  "--color-info": string
  "--color-info-content": string
  "--color-success": string
  "--color-success-content": string
  "--color-warning": string
  "--color-warning-content": string
  "--color-error": string
  "--color-error-content": string
  "--radius-selector": string
  "--radius-field": string
  "--radius-box": string
  "--size-selector": string
  "--size-field": string
  "--border": string
  "--depth": string
  "--noise": string
}


interface Themes {
  retro: Theme
  bumblebee: Theme
  fantasy: Theme
  aqua: Theme
  garden: Theme
  coffee: Theme
  pastel: Theme
  halloween: Theme
  cupcake: Theme
  lofi: Theme
  emerald: Theme
  night: Theme
  dark: Theme
  synthwave: Theme
  light: Theme
  silk: Theme
  nord: Theme
  dim: Theme
  business: Theme
  corporate: Theme
  acid: Theme
  caramellatte: Theme
  sunset: Theme
  forest: Theme
  dracula: Theme
  autumn: Theme
  wireframe: Theme
  luxury: Theme
  abyss: Theme
  valentine: Theme
  cyberpunk: Theme
  lemonade: Theme
  winter: Theme
  black: Theme
  cmyk: Theme
  [key: string]: Theme
}

declare const themes: Themes
export default themes