export interface Waste {
  metadata: {
    contentType: string;
    filename: string;
  }
  data: Blob,
}