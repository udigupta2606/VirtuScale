u16 lowerBound(span<u8> skey, bool& foundExactOut) {
   foundExactOut = false;
   int cmp = memcmp(skey.data(), getPrefix(), min(skey.size(), prefixLen));
   if (cmp < 0 || (cmp == 0 && skey.size() < prefixLen))
      return 0;
   if (cmp > 0)
      return count;

   u8* key = skey.data() + prefixLen;
   unsigned keyLen = skey.size() - prefixLen;
   u16 lower = 0, upper = count;
   u32 keyHead = head(key, keyLen);
   searchHint(keyHead, lower, upper);

   while (lower < upper) {
      u16 mid = (upper + lower) / 2;
      if (keyHead < slot[mid].head)
         upper = mid;
      else if (keyHead > slot[mid].head)
         lower = mid + 1;
      else {
         int cmp = memcmp(key, getKey(mid), min(keyLen, slot[mid].keyLen));
         if (cmp < 0)
            upper = mid;
         else if (cmp > 0)
            lower = mid + 1;
         else if (keyLen < slot[mid].keyLen)
            upper = mid;
         else if (keyLen > slot[mid].keyLen)
            lower = mid + 1;
         else {
            foundExactOut = true;
            return mid;
         }
      }
   }
   return lower;
}

u16 lowerBound(span<u8> key) {
   bool ignore;
   return lowerBound(key, ignore);
}

void insertInPage(span<u8> key, span<u8> payload) {
   if (spaceNeeded(key.size(), payload.size()) > freeSpace()) {
      assert(spaceNeeded(key.size(), payload.size()) <= freeSpaceAfterCompaction());
      compactify();
   }
   unsigned slotId = lowerBound(key);
   memmove(slot + slotId + 1, slot + slotId, sizeof(Slot) * (count - slotId));
   storeKeyValue(slotId, key, payload);
   count++;
   updateHint(slotId);
}

bool removeSlot(unsigned slotId) {
   spaceUsed -= slot[slotId].keyLen + slot[slotId].payloadLen;
   memmove(slot + slotId, slot + slotId + 1, sizeof(Slot) * (count - slotId - 1));
   count--;
   makeHint();
   return true;
}

bool removeInPage(span<u8> key) {
   bool found;
   unsigned slotId = lowerBound(key, found);
   return found ? removeSlot(slotId) : false;
}

void copyNode(BTreeNodeHeader* dst, BTreeNodeHeader* src) {
   u64 ofs = offsetof(BTreeNodeHeader, upperInnerNode);
   memcpy(reinterpret_cast<u8*>(dst) + ofs, reinterpret_cast<u8*>(src) + ofs, sizeof(BTreeNode) - ofs);
}

void compactify() {
   unsigned should = freeSpaceAfterCompaction();
   BTreeNode tmp(isLeaf);
   tmp.setFences(getLowerFence(), getUpperFence());
   copyKeyValueRange(&tmp, 0, 0, count);
   tmp.upperInnerNode = upperInnerNode;
   copyNode(this, &tmp);
   makeHint();
   assert(freeSpace() == should);
}

bool mergeNodes(unsigned slotId, BTreeNode* parent, BTreeNode* right) {
   if (!isLeaf) return true;

   BTreeNode tmp(isLeaf);
   tmp.setFences(getLowerFence(), right->getUpperFence());

   unsigned leftGrow = (prefixLen - tmp.prefixLen) * count;
   unsigned rightGrow = (right->prefixLen - tmp.prefixLen) * right->count;
   unsigned spaceUpperBound = spaceUsed + right->spaceUsed +
       (reinterpret_cast<u8*>(slot + count + right->count) - ptr()) + leftGrow + rightGrow;

   if (spaceUpperBound > pageSize) return false;

   copyKeyValueRange(&tmp, 0, 0, count);
   right->copyKeyValueRange(&tmp, count, 0, right->count);

   PID pid = bm.toPID(this);
   memcpy(parent->getPayload(slotId + 1).data(), &pid, sizeof(PID));
   parent->removeSlot(slotId);

   tmp.makeHint();
   tmp.nextLeafNode = right->nextLeafNode;
   copyNode(this, &tmp);
   return true;
}

void storeKeyValue(u16 slotId, span<u8> skey, span<u8> payload) {
   u8* key = skey.data() + prefixLen;
   unsigned keyLen = skey.size() - prefixLen;
   slot[slotId].head = head(key, keyLen);
   slot[slotId].keyLen = keyLen;
   slot[slotId].payloadLen = payload.size();
   unsigned space = keyLen + payload.size();
   dataOffset -= space;
   spaceUsed += space;
   slot[slotId].offset = dataOffset;
   memcpy(getKey(slotId), key, keyLen);
   memcpy(getPayload(slotId).data(), payload.data(), payload.size());
}

void copyKeyValueRange(BTreeNode* dst, u16 dstSlot, u16 srcSlot, unsigned srcCount) {
   if (prefixLen <= dst->prefixLen) {
      unsigned diff = dst->prefixLen - prefixLen;
      for (unsigned i = 0; i < srcCount; i++) {
         unsigned newKeyLen = slot[srcSlot + i].keyLen - diff;
         unsigned space = newKeyLen + slot[srcSlot + i].payloadLen;
         dst->dataOffset -= space;
         dst->spaceUsed += space;
         dst->slot[dstSlot + i].offset = dst->dataOffset;
         u8* key = getKey(srcSlot + i) + diff;
         memcpy(dst->getKey(dstSlot + i), key, space);
         dst->slot[dstSlot + i].head = head(key, newKeyLen);
         dst->slot[dstSlot + i].keyLen = newKeyLen;
         dst->slot[dstSlot + i].payloadLen = slot[srcSlot + i].payloadLen;
      }
   } else {
      for (unsigned i = 0; i < srcCount; i++)
         copyKeyValue(srcSlot + i, dst, dstSlot + i);
   }
   dst->count += srcCount;
   assert((dst->ptr() + dst->dataOffset) >= reinterpret_cast<u8*>(dst->slot + dst->count));
}

void copyKeyValue(u16 srcSlot, BTreeNode* dst, u16 dstSlot) {
   unsigned fullLen = slot[srcSlot].keyLen + prefixLen;
   u8 key[fullLen];
   memcpy(key, getPrefix(), prefixLen);
   memcpy(key + prefixLen, getKey(srcSlot), slot[srcSlot].keyLen);
   dst->storeKeyValue(dstSlot, {key, fullLen}, getPayload(srcSlot));
}

void insertFence(FenceKeySlot& fk, span<u8> key) {
   dataOffset -= key.size();
   spaceUsed += key.size();
   fk.offset = dataOffset;
   fk.len = key.size();
   memcpy(ptr() + dataOffset, key.data(), key.size());
}

void setFences(span<u8> lower, span<u8> upper) {
   insertFence(lowerFence, lower);
   insertFence(upperFence, upper);
   prefixLen = 0;
   while (prefixLen < min(lower.size(), upper.size()) && lower[prefixLen] == upper[prefixLen])
      prefixLen++;
}

void splitNode(BTreeNode* parent, unsigned sepSlot, span<u8> sep) {
   assert(sepSlot > 0 && sepSlot < (pageSize / sizeof(PID)));

   BTreeNode tmp(isLeaf);
   BTreeNode* nodeLeft = &tmp;
   AllocGuard<BTreeNode> newNode(isLeaf);
   BTreeNode* nodeRight = newNode.ptr;

   nodeLeft->setFences(getLowerFence(), sep);
   nodeRight->setFences(sep, getUpperFence());

   PID leftPID = bm.toPID(this);
   u16 oldParentSlot = parent->lowerBound(sep);
   if (oldParentSlot == parent->count) {
      assert(parent->upperInnerNode == leftPID);
      parent->upperInnerNode = newNode.pid;
   } else {
      assert(parent->getChild(oldParentSlot) == leftPID);
      memcpy(parent->getPayload(oldParentSlot).data(), &newNode.pid, sizeof(PID));
   }
   parent->insertInPage(sep, {reinterpret_cast<u8*>(&leftPID), sizeof(PID)});

   if (isLeaf) {
      copyKeyValueRange(nodeLeft, 0, 0, sepSlot + 1);
      copyKeyValueRange(nodeRight, 0, nodeLeft->count, count - nodeLeft->count);
      nodeLeft->nextLeafNode = newNode.pid;
      nodeRight->nextLeafNode = this->nextLeafNode;
   } else {
      copyKeyValueRange(nodeLeft, 0, 0, sepSlot);
      copyKeyValueRange(nodeRight, 0, nodeLeft->count + 1, count - nodeLeft->count - 1);
      nodeLeft->upperInnerNode = getChild(nodeLeft->count);
      nodeRight->upperInnerNode = upperInnerNode;
   }

   nodeLeft->makeHint();
   nodeRight->makeHint();
   copyNode(this, nodeLeft);
}

unsigned commonPrefix(unsigned slotA, unsigned slotB) {
   unsigned limit = min(slot[slotA].keyLen, slot[slotB].keyLen);
   u8 *a = getKey(slotA), *b = getKey(slotB);
   unsigned i = 0;
   while (i < limit && a[i] == b[i]) i++;
   return i;
}

struct SeparatorInfo {
   unsigned len;
   unsigned slot;
   bool isTruncated;
};

SeparatorInfo findSeparator(bool splitOrdered) {
   assert(count > 1);
   if (isInner())
      return {static_cast<unsigned>(prefixLen + slot[count / 2].keyLen), count / 2, false};

   unsigned bestSlot;
   if (splitOrdered)
      bestSlot = count - 2;
   else if (count > 16) {
      unsigned lower = (count / 2) - (count / 16), upper = count / 2;
      unsigned bestPrefixLen = commonPrefix(lower, 0);
      bestSlot = lower;
      if (bestPrefixLen != commonPrefix(upper - 1, 0)) {
         for (unsigned i = lower + 1; i < upper && commonPrefix(i, 0) == bestPrefixLen; i++)
            bestSlot = i;
      }
   } else
      bestSlot = (count - 1) / 2;

   unsigned common = commonPrefix(bestSlot, bestSlot + 1);
   if (slot[bestSlot].keyLen > common && slot[bestSlot + 1].keyLen > common + 1)
      return {prefixLen + common + 1, bestSlot, true};

   return {static_cast<unsigned>(prefixLen + slot[bestSlot].keyLen), bestSlot, false};
}

void getSep(u8* sepKeyOut, SeparatorInfo info) {
   memcpy(sepKeyOut, getPrefix(), prefixLen);
   memcpy(sepKeyOut + prefixLen, getKey(info.slot + info.isTruncated), info.len - prefixLen);
}

PID lookupInner(span<u8> key) {
   unsigned pos = lowerBound(key);
   return (pos == count) ? upperInnerNode : getChild(pos);
}