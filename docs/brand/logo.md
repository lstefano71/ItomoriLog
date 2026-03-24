# ✅ **1. ItomoriLog – SVG Logo (Primary)**

Dark‑theme optimized.  
Colors: Sakura Pink `#FF6DAE`, Neon Cyan `#0EA5E9`.

This is a **braided comet/timeline** mark forming an **abstract “I” + braided threads** — symbolizing the merging of multiple log timelines.

> ✅ Safe to drop into `assets/logo/itomorilog.svg`

```svg
<svg width="256" height="256" viewBox="0 0 256 256"
     xmlns="http://www.w3.org/2000/svg" version="1.1">

  <!-- Background circle (optional; remove for transparent version) -->
  <circle cx="128" cy="128" r="118" fill="#111418" stroke="#1F2430" stroke-width="4"/>

  <!-- Braided comet/timeline strokes -->
  <path d="
      M 60 170
      C 90 130, 110 110, 128 80
      C 146 110, 166 130, 196 170"
      fill="none"
      stroke="#FF6DAE"
      stroke-width="10"
      stroke-linecap="round"
      stroke-linejoin="round"
  />

  <path d="
      M 60 150
      C 90 115, 110 95, 128 70
      C 146 95, 166 115, 196 150"
      fill="none"
      stroke="#0EA5E9"
      stroke-width="6"
      stroke-linecap="round"
      stroke-linejoin="round"
      opacity="0.9"
  />

  <!-- Dot representing "origin" of the timeline -->
  <circle cx="128" cy="60" r="8" fill="#FF6DAE"/>

  <!-- Name -->
  <text x="128" y="218"
        font-family="Inter,Segoe UI,Arial"
        font-size="28"
        fill="#D6E2F0"
        text-anchor="middle"
        font-weight="600">
    ItomoriLog
  </text>

</svg>
```

✅ Works at **16px → 512px**  
✅ No filters (safe for Avalonia, embedded resources, AOT)  
✅ Clean geometric curves (mecha‑anime feeling without being kitsch)

