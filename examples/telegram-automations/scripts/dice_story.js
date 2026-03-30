export async function run(ctx) {
  return { roll: ctx.rollDie(6) };
}
