export default function useBaseUrl() {
  if (import.meta.env.PROD) {
    return '/';
  } else {
    return 'http://localhost:3514/';
  }
}