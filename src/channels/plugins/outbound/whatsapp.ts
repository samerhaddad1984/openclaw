import { chunkText } from "../../../auto-reply/chunk.js";
import { shouldLogVerbose } from "../../../globals.js";
import { sendPollWhatsApp } from "../../../web/outbound.js";
import { resolveWhatsAppOutboundTarget } from "../../../whatsapp/resolve-outbound-target.js";
import type { ChannelOutboundAdapter } from "../types.js";

import { sendTextMediaPayload } from "./direct-text-media.js";
import { validateLedgerLinkOutputOrThrow } from "./ledgerlink-validator.js";

function ledgerLinkGuard(text: unknown) {
  if (typeof text === "string" && text.trim().length > 0) {
    validateLedgerLinkOutputOrThrow(text, "whatsapp");
  }
}

export const whatsappOutbound: ChannelOutboundAdapter = {
  deliveryMode: "gateway",
  chunker: chunkText,
  chunkerMode: "text",
  textChunkLimit: 4000,
  pollMaxOptions: 12,

  resolveTarget: ({ to, allowFrom, mode }) =>
    resolveWhatsAppOutboundTarget({ to, allowFrom, mode }),

  sendPayload: (ctx) => sendTextMediaPayload({ channel: "whatsapp", ctx, adapter: whatsappOutbound }),

  sendText: async ({ to, text, accountId, deps, gifPlayback }) => {
    // LedgerLink CPA Guardrail: block numeric output without provenance
    ledgerLinkGuard(text);

    const send =
      deps?.sendWhatsApp ?? (await import("../../../web/outbound.js")).sendMessageWhatsApp;

    const result = await send(to, text, {
      verbose: false,
      accountId: accountId ?? undefined,
      gifPlayback,
    });

    return { channel: "whatsapp", ...result };
  },

  sendMedia: async ({ to, text, mediaUrl, mediaLocalRoots, accountId, deps, gifPlayback }) => {
    // LedgerLink CPA Guardrail: block numeric output without provenance
    ledgerLinkGuard(text);

    const send =
      deps?.sendWhatsApp ?? (await import("../../../web/outbound.js")).sendMessageWhatsApp;

    const result = await send(to, text ?? "", {
      verbose: false,
      mediaUrl,
      mediaLocalRoots,
      accountId: accountId ?? undefined,
      gifPlayback,
    });

    return { channel: "whatsapp", ...result };
  },

  sendPoll: async ({ to, poll, accountId }) =>
    sendPollWhatsApp(to, poll, {
      verbose: shouldLogVerbose(),
      accountId: accountId ?? undefined,
    }),
};