export const secondsFromHours = (hours: number): number => hours * 60 * 60;

export const millisFromSeconds = (seconds: number): number => seconds * 1000;

export const millisFromMinutes = (minutes: number): number =>
  minutes * 60 * 1000;

export const millisFromHours = (hours: number): number =>
  hours * 60 * 60 * 1000;

export const millisFromDays = (days: number): number =>
  days * 24 * 60 * 60 * 1000;
