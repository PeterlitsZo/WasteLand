import useBaseUrl from './useBaseUrl';
import { useQuery } from '@tanstack/react-query';
import axios from 'axios';

import { WasteId } from '../utils/WasteId';

interface ListWasteResponse {
  data: WasteId[],
}

export default function useListWastes() {
  const url = useBaseUrl() + 'api/v1/wastes';
  const { isLoading, error, data } = useQuery({
    queryKey: ['wastes', 'list'],
    queryFn: async () => {
      let result = await axios.get(url);
      let data = result.data as ListWasteResponse;
      return data;
    }
  });
  return { isLoading, error, data: data?.data }
}