import telegramLogo from "../../../../assets/channels/telegram.png";
import whatsappLogo from "../../../../assets/channels/whatsapp.png";
import anthropicLogo from "../../../../assets/providers/anthropic.png";
import customLogo from "../../../../assets/providers/custom.png";
import geminiLogo from "../../../../assets/providers/gemini.png";
import groqLogo from "../../../../assets/providers/groq.png";
import mistralLogo from "../../../../assets/providers/mistral.png";
import openaiLogo from "../../../../assets/providers/openai.png";
import openrouterLogo from "../../../../assets/providers/openrouter.png";
import togetherLogo from "../../../../assets/providers/togetherai.png";
import zaiLogo from "../../../../assets/providers/zai.png";
import braveLogo from "../../../../assets/search/brave.png";
import firecrawlLogo from "../../../../assets/search/firecrawl.png";

export const channelLogos = {
  telegram: telegramLogo,
  whatsapp: whatsappLogo,
} as const;

export const providerLogos: Record<string, string> = {
  anthropic: anthropicLogo,
  custom: customLogo,
  google: geminiLogo,
  groq: groqLogo,
  mistral: mistralLogo,
  openai: openaiLogo,
  openrouter: openrouterLogo,
  together: togetherLogo,
  zai: zaiLogo,
};

export const searchLogos = {
  brave: braveLogo,
  firecrawl: firecrawlLogo,
} as const;
