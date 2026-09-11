import { ChartJSNodeCanvas } from "chartjs-node-canvas";
import Annotation from "chartjs-plugin-annotation";

// chartjs-plugin-annotation must be registered exactly once per ChartJSNodeCanvas instance.
// Creating multiple instances and re-registering corrupts the plugin's resolver state.
// Solution: keep one singleton per size bucket.
const instances = new Map();

export function getCanvas(width, height) {
  const key = `${width}x${height}`;
  if (!instances.has(key)) {
    instances.set(key, new ChartJSNodeCanvas({
      width,
      height,
      backgroundColour: "white",
      chartCallback: (ChartJS) => {
        ChartJS.register(Annotation);
      },
    }));
  }
  return instances.get(key);
}
