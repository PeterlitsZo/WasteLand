import useBaseUrl from './useBaseUrl';
import { useQuery } from '@tanstack/react-query';

import { WasteId } from '../utils/WasteId';

interface GetWasteResponse {
  metadata: {
    contentType: string;
  }
  data: Blob;
}

export default function useGetWaste(id: WasteId) {
  const url = useBaseUrl() + 'api/v1/wastes';
  const { isLoading, error, data } = useQuery({
    queryKey: ['wastes', 'get', id],
    queryFn: async () => {
      let result = await fetch(url + '/' + id);
      let contentType = (result.headers.get('Content-Type') ?? '') as string;
      let data = {
        metadata: {
          contentType,
        },
        data: new Blob([await result.arrayBuffer()], { type: contentType }),
      } as GetWasteResponse;
      return data;
    }
  });
  return { isLoading, error, data: data }
}