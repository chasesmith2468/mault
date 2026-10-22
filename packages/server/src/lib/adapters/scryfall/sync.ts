import { createInterface } from "node:readline";
import { Readable, Transform } from "node:stream";
import type { ReadableStream as NodeWebReadableStream } from "node:stream/web";
import { createGunzip } from "node:zlib";
import type { SyncSource, SyncSourceCard } from "../../interfaces/card-search";
import { CARD_API_HEADERS } from "../../constants/card-search";
import {
  BULK_DOWNLOAD_LOG_EVERY_BYTES,
  BULK_DOWNLOAD_LOG_EVERY_CARDS,
  SCRYFALL_VOLATILE_DATA_KEYS,
} from "../../constants/sync";
import { SCRYFALL_DEFAULT_URL } from "../../constants/urls";
import type { ScryfallBulkCard } from "../../interfaces/scryfall";

function megabytes(bytes: number): string {
  return `${Math.max(1, Math.round(bytes / 1_048_576))} MB`;
}

function cardImageUrl(
  raw: Pick<ScryfallBulkCard, "image_uris" | "card_faces">,
) {
  const uris = raw.image_uris ?? raw.card_faces?.[0]?.image_uris;
  return uris?.png ?? uris?.large;
}

function cardBackImageUrl(
  raw: Pick<ScryfallBulkCard, "image_uris" | "card_faces">,
) {
  if (raw.image_uris) return undefined;
  const uris = raw.card_faces?.[1]?.image_uris;
  return uris?.png ?? uris?.large;
}

function apiRoot(baseUrl: string): string {
  try {
    return new URL(baseUrl).origin;
  } catch {
    return new URL(SCRYFALL_DEFAULT_URL).origin;
  }
}

async function downloadBulkData(
  baseUrl: string,
  addLog: (msg: string) => void,
  bulkType: string,
  lang: string | undefined,
  signal?: AbortSignal,
): Promise<SyncSourceCard[]> {
  addLog("Fetching Scryfall bulk data catalog...");

  const catalogRes = await fetch(`${apiRoot(baseUrl)}/bulk-data`, {
    headers: CARD_API_HEADERS,
    signal,
  });
  if (!catalogRes.ok) {
    throw new Error(`Scryfall catalog fetch failed: ${catalogRes.status}`);
  }
  const catalog = (await catalogRes.json()) as {
    data: { type: string; jsonl_download_uri: string }[];
  };

  const entry = catalog.data.find((e) => e.type === bulkType);
  if (!entry) throw new Error(`Could not find ${bulkType} bulk data entry`);

  addLog(`Downloading bulk "${bulkType}" data...`);

  const bulkRes = await fetch(entry.jsonl_download_uri, {
    headers: CARD_API_HEADERS,
    signal,
  });
  if (!bulkRes.ok || !bulkRes.body)
    throw new Error(`Bulk data download failed: ${bulkRes.status}`);

  const expected = Number(bulkRes.headers.get("content-length"));
  if (expected > 0) addLog(`Bulk file is ${megabytes(expected)}.`);

  const cards: SyncSourceCard[] = [];
  let seen = 0;
  let received = 0;
  let nextByteLog = BULK_DOWNLOAD_LOG_EVERY_BYTES;
  const counter = new Transform({
    transform(chunk: Buffer | Uint8Array, _encoding, callback) {
      received += chunk.length;
      if (received >= nextByteLog) {
        nextByteLog += BULK_DOWNLOAD_LOG_EVERY_BYTES;
        addLog(
          expected > 0
            ? `Downloaded ${megabytes(received)} of ${megabytes(expected)}.`
            : `Downloaded ${megabytes(received)}.`,
        );
      }
      callback(null, chunk);
    },
  });
  const lines = createInterface({
    input: Readable.fromWeb(bulkRes.body as NodeWebReadableStream<Uint8Array>)
      .pipe(counter)
      .pipe(createGunzip()),
    crlfDelay: Infinity,
  });

  for await (const line of lines) {
    if (!line) continue;
    seen++;
    if (seen % BULK_DOWNLOAD_LOG_EVERY_CARDS === 0) {
      addLog(`Read ${seen} cards from the bulk file.`);
    }
    const raw = JSON.parse(line) as ScryfallBulkCard;
    if (lang && raw.lang !== lang) continue;
    cards.push({
      id: raw.id,
      name: raw.printed_name ?? raw.name,
      setCode: raw.set,
      imageUrl: cardImageUrl(raw),
      backImageUrl: cardBackImageUrl(raw),
      data: line,
    });
  }

  addLog(`Scanned ${seen} cards, kept ${cards.length}.`);

  return cards;
}

async function fetchCards(
  baseUrl: string,
  addLog: (msg: string) => void,
  lang: string = "en",
  signal?: AbortSignal,
): Promise<SyncSourceCard[]> {
  return lang === "en"
    ? downloadBulkData(baseUrl, addLog, "default_cards", "en", signal)
    : downloadBulkData(baseUrl, addLog, "all_cards", lang, signal);
}

async function fetchOne(id: string, baseUrl: string) {
  const url = `${baseUrl}/${id}`;
  const res = await fetch(url, { headers: CARD_API_HEADERS });
  if (!res.ok) return { card: null, urls: [`${url} [HTTP ${res.status}]`] };
  const data = await res.text();
  const raw = JSON.parse(data) as ScryfallBulkCard;
  return {
    card: {
      name: raw.name,
      setCode: raw.set,
      imageUrl: cardImageUrl(raw),
      backImageUrl: cardBackImageUrl(raw),
      data,
    },
    urls: [url],
  };
}

export const scryfallSyncSource: SyncSource = {
  gameKey: "mtg",
  label: "Magic: The Gathering (Scryfall)",
  defaultUrl: SCRYFALL_DEFAULT_URL,
  fetchHeaders: CARD_API_HEADERS,
  languages: [
    "en",
    "es",
    "fr",
    "de",
    "it",
    "pt",
    "ja",
    "lo",
    "ru",
    "zhs",
    "zht",
  ],
  volatileDataKeys: SCRYFALL_VOLATILE_DATA_KEYS,
  fetchCards,
  fetchOne,
};
