export const sum = (nums: readonly number[]) =>
  nums.reduce((sum, num) => sum + num, 0);
