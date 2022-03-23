export function toU64String(value: number): string {
    if (!Number.isInteger(value)) {
        throw new Error(`Value ${value} is not integer`);
    }

    const hex_string = value.toString(16);
    const u64String = (hex_string.length - 1).toString(16) + hex_string;
    return u64String;
}
