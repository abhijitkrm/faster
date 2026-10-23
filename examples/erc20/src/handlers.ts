import { ERC20, type Account } from "../generated/index.ts";

const empty = (id: string): Account => ({ id, balance: 0n, transfersIn: 0, transfersOut: 0 });

ERC20.Transfer.handler(async ({ event, context }) => {
  const { from, to, value } = event.params;
  const a = await context.Account.getOrCreate(from.toLowerCase(), () => empty(from.toLowerCase()));
  context.Account.set({ ...a, balance: a.balance - value, transfersOut: a.transfersOut + 1 });
  const b = await context.Account.getOrCreate(to.toLowerCase(), () => empty(to.toLowerCase()));
  context.Account.set({ ...b, balance: b.balance + value, transfersIn: b.transfersIn + 1 });
  context.Transfer.set({
    id: `${event.block.number}-${event.logIndex}`,
    from: from.toLowerCase(),
    to: to.toLowerCase(),
    value,
    block: event.block.number,
    timestamp: event.block.timestamp,
    txHash: event.transaction.hash,
  });
});
