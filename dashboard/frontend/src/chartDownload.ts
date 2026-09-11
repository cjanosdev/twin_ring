/**
 * Download every chart in `refs` sequentially, staggered by 300ms so browsers
 * don't block the downloads. `prefix` is prepended to each filename.
 */
export async function downloadAllCharts(
  refs: Array<{ ref: React.MutableRefObject<HTMLDivElement | null>; name: string }>,
  prefix: string
): Promise<void> {
  for (const { ref, name } of refs) {
    await downloadChartPng(ref, `${prefix}_${name}`);
    await new Promise((r) => setTimeout(r, 300));
  }
}

/**
 * Download the chart inside a div as a PNG using the Canvas API.
 * Works by serialising the SVG that Recharts renders, drawing it onto a canvas,
 * then triggering a download.
 */
export async function downloadChartPng(
  containerRef: React.MutableRefObject<HTMLDivElement | null>,
  filename: string
): Promise<void> {
  const container = containerRef.current;
  if (!container) return;

  const svg = container.querySelector("svg");
  if (!svg) return;

  const svgClone = svg.cloneNode(true) as SVGSVGElement;

  // Bake computed styles into the clone so the rasterised version matches
  const width = svg.clientWidth || svg.viewBox.baseVal.width || 800;
  const height = svg.clientHeight || svg.viewBox.baseVal.height || 300;
  svgClone.setAttribute("width", String(width));
  svgClone.setAttribute("height", String(height));

  // Set a background so PNGs aren't transparent
  const bg = document.createElementNS("http://www.w3.org/2000/svg", "rect");
  bg.setAttribute("width", "100%");
  bg.setAttribute("height", "100%");
  bg.setAttribute("fill", "#0f172a");
  svgClone.insertBefore(bg, svgClone.firstChild);

  const svgStr = new XMLSerializer().serializeToString(svgClone);
  const blob = new Blob([svgStr], { type: "image/svg+xml;charset=utf-8" });
  const url = URL.createObjectURL(blob);

  const img = new Image();
  img.src = url;

  await new Promise<void>((resolve) => {
    img.onload = () => {
      const canvas = document.createElement("canvas");
      // 2× for retina sharpness
      canvas.width = width * 2;
      canvas.height = height * 2;
      const ctx = canvas.getContext("2d")!;
      ctx.scale(2, 2);
      ctx.drawImage(img, 0, 0);
      URL.revokeObjectURL(url);

      canvas.toBlob((pngBlob) => {
        if (!pngBlob) return;
        const a = document.createElement("a");
        a.href = URL.createObjectURL(pngBlob);
        a.download = `${filename}.png`;
        a.click();
        URL.revokeObjectURL(a.href);
        resolve();
      }, "image/png");
    };
    img.onerror = () => {
      URL.revokeObjectURL(url);
      resolve();
    };
  });
}
