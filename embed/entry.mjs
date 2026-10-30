import { BACKGROUND_CONTEXT } from "@earendil-works/chord/context";
import { createModels } from "@earendil-works/pi-ai/models";
import { fauxProvider, fauxAssistantMessage, fauxToolCall } from "@earendil-works/pi-ai/providers/faux";
import { Type } from "@earendil-works/pi-ai";
import { createRegistry, defineExtension, defineTool, Harness, MemoryStorage, AssistantEntry } from "@earendil-works/pi-durable";
import { createToolSearchExtension } from "@earendil-works/pi-durable/tool-search";
globalThis.PI = { BACKGROUND_CONTEXT, createModels, fauxProvider, fauxAssistantMessage, fauxToolCall, Type, createRegistry, defineExtension, defineTool, Harness, MemoryStorage, AssistantEntry, createToolSearchExtension };
