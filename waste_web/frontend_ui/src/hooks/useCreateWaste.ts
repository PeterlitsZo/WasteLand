import useBaseUrl from './useBaseUrl';
import { useMutation } from '@tanstack/react-query';
import axios from 'axios';

import { Waste } from '../utils/Waste';

interface CreateWasteResponse {}

export default function useCreateWastes() {
  const url = useBaseUrl() + 'api/v1/wastes';
  const { isLoading, mutate, error, data } = useMutation({
    mutationKey: ['wastes', 'create'],
    mutationFn: async (wastes: Waste[]) => {
      for (const waste of wastes) {
        await axios.post(
          url,
          waste.data,
          { headers: { 'Content-Type': waste.metadata.contentType } }
        );
      }
      return {} as CreateWasteResponse;
    }
  });
  return { isLoading, mutate, error, data }
}