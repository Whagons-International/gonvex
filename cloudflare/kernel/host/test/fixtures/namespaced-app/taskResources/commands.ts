import { action } from "@gonvex/module-sdk";

export const generateUploadUrl = action({
  run: async (ctx) => ({ uploadUrl: `https://uploads.invalid/${ctx.function}` }),
});
