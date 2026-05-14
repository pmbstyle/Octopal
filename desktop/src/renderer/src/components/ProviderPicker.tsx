import { ChevronLeft, ChevronRight } from "lucide-react";
import { useRef } from "react";

import { providers } from "../lib/install";
import { providerLogos } from "../lib/logos";
import { ImageLogo } from "./ImageLogo";
import { ProviderLogo } from "./ProviderLogo";

export function ProviderPicker({
  selected,
  onSelect,
}: {
  selected: string;
  onSelect: (providerId: string) => void;
}) {
  const trackRef = useRef<HTMLDivElement>(null);

  function move(direction: -1 | 1) {
    const track = trackRef.current;
    if (!track) {
      return;
    }
    track.scrollBy({ left: direction * Math.min(520, track.clientWidth * 0.82), behavior: "smooth" });
  }

  return (
    <div className="provider-carousel">
      <button className="carousel-arrow" type="button" aria-label="Previous providers" onClick={() => move(-1)}>
        <ChevronLeft />
      </button>
      <div className="provider-grid" ref={trackRef}>
        {providers.map((provider) => (
          <button
            key={provider.id}
            type="button"
            className={provider.id === selected ? "provider-card provider-card-active" : "provider-card"}
            onClick={() => onSelect(provider.id)}
          >
            {providerLogos[provider.id] ? (
              <ImageLogo className="provider-image-logo" src={providerLogos[provider.id]} alt="" />
            ) : (
              <ProviderLogo label={provider.label.slice(0, 2).toUpperCase()} />
            )}
            <strong>{provider.label}</strong>
            <small>{provider.model || "Custom model"}</small>
          </button>
        ))}
      </div>
      <button className="carousel-arrow" type="button" aria-label="Next providers" onClick={() => move(1)}>
        <ChevronRight />
      </button>
    </div>
  );
}
