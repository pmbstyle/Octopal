import { cn } from "../lib/cn";

export function ImageLogo({ src, alt, className }: { src: string; alt: string; className?: string }) {
  return <img className={cn("image-logo", className)} src={src} alt={alt} />;
}
