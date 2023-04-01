#include <memory>
#include <sstream>
#include <string>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/macros.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/b_plus_tree_internal_page.h"
#include "storage/page/b_plus_tree_leaf_page.h"
#include "storage/page/b_plus_tree_page.h"
#include "storage/page/page.h"
#include "storage/page/page_guard.h"

namespace bustub {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, page_id_t header_page_id, BufferPoolManager *buffer_pool_manager,
                          const KeyComparator &comparator, int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      bpm_(buffer_pool_manager),
      comparator_(std::move(comparator)),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size),
      header_page_id_(header_page_id) {
  WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  root_page->root_page_id_ = INVALID_PAGE_ID;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::KeyIndex(const KeyType &key,
                              const BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *node) const -> int {
  int l = 0;
  int r = node->GetSize() - 1;
  while (l < r) {
    int mid = (l + r) >> 1;
    if (!(comparator_(node->KeyAt(mid), key) == -1)) {  // key < right
      r = mid;
    } else {
      l = mid + 1;
    }
  }
  return l;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::KeyIndex(const KeyType &key,
                              const BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *node) const -> int {
  int l = 0;
  int r = node->GetSize() - 1;
  while (l < r) {
    int mid = (l + r) >> 1;
    if (!(comparator_(node->KeyAt(mid), key) == -1)) {  // key < right
      r = mid;
    } else {
      l = mid + 1;
    }
  }
  return l;
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SplitNode(BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *fullnode,
                               BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *newnode) {
  int new_node_size = fullnode->GetSize() / 2;
  int old_node_size = fullnode->GetSize() - new_node_size;

  for (int ind_old = old_node_size, ind_new = 0; ind_old < fullnode->GetSize(); ind_old++, ind_new++) {
    newnode->ArrayAt(ind_new) = fullnode->ArrayAt(ind_old);
  }

  fullnode->SetSize(old_node_size);
  newnode->SetSize(new_node_size);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::SplitNode(BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *fullnode,
                               BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *newnode, int insert_pos,
                               const KeyType &key, page_id_t val) {
  int old_node_size = (fullnode->GetSize() + 1) / 2;
  int new_node_size = (fullnode->GetSize() + 1) - old_node_size;
  // 如果新来的key在老节点中保存
  if (insert_pos < old_node_size) {
    for (int ind_old = old_node_size - 1, ind_new = 0; ind_old < fullnode->GetSize(); ind_old++, ind_new++) {
      newnode->ArrayAt(ind_new) = fullnode->ArrayAt(ind_old);
    }
    fullnode->SetSize(old_node_size - 1);
    InsertKey(key, val, fullnode, insert_pos);
  } else {
    for (int ind_old = old_node_size, ind_new = 0; ind_old < fullnode->GetSize(); ind_old++, ind_new++) {
      newnode->ArrayAt(ind_new) = fullnode->ArrayAt(ind_old);
    }
    newnode->SetSize(new_node_size - 1);
    InsertKey(key, val, newnode, insert_pos - old_node_size);
  }
  fullnode->SetSize(old_node_size);
  newnode->SetSize(new_node_size);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertKey(const KeyType &key, const ValueType &val,
                               BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *node, int insertPos) {
  for (int j = node->GetSize(); j > insertPos; j--) {
    node->ArrayAt(j) = node->ArrayAt(j - 1);
  }
  node->SetKeyAt(insertPos, key);
  node->SetValueAt(insertPos, val);
  node->IncreaseSize(1);
}
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertKey(const KeyType &key, const page_id_t &val,
                               BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *node, int insertPos) {
  for (int j = node->GetSize(); j > insertPos; j--) {
    node->ArrayAt(j) = node->ArrayAt(j - 1);
  }
  node->SetKeyAt(insertPos, key);
  node->SetValueAt(insertPos, val);
  node->IncreaseSize(1);
}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return this->GetRootPageId() == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *txn) -> bool {
  if (IsEmpty()) {
    return false;
  }
  // Declaration of context instance.
  Context ctx;
  WritePageGuard guard = bpm_->FetchPageWrite(header_page_id_);
  auto root_page = guard.AsMut<BPlusTreeHeaderPage>();
  ctx.root_page_id_ = root_page->root_page_id_;
  ctx.header_page_ = std::move(guard);

  ReadPageGuard iter_pageguard = bpm_->FetchPageRead(ctx.root_page_id_);
  auto iter_page = iter_pageguard.As<BPlusTreePage>();

  while (!iter_page->IsLeafPage()) {
    auto tmp_inter_page = iter_pageguard.As<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
    int i;
    // for (i = 0; i < tmp_inter_page->GetSize(); i++) {
    //   if (comparator_(key, tmp_inter_page->KeyAt(i)) != 1) {
    //     break ;
    //   }
    // }
    i = this->KeyIndex(key, tmp_inter_page);
    // finded
    if (comparator_(key, tmp_inter_page->KeyAt(i)) != 1) {
      ctx.read_set_.push_back(std::move(iter_pageguard));
      iter_pageguard = bpm_->FetchPageRead(tmp_inter_page->ValueAt(i));
      iter_page = iter_pageguard.As<BPlusTreePage>();
    } else {
      return false;
    }
  }
  auto leaf_page = iter_pageguard.As<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();

  int i = this->KeyIndex(key, leaf_page);
  if (comparator_(leaf_page->KeyAt(i), key) == 0) {
    if (result != nullptr) {
      result->push_back(leaf_page->ValueAt(i));
    }
    return true;
  }
  return false;

  // (void)ctx;
  // return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *txn) -> bool {
  // Declaration of context instance.
  Context ctx;
  ctx.header_page_ = bpm_->FetchPageWrite(header_page_id_);
  auto *header_page = ctx.header_page_->AsMut<BPlusTreeHeaderPage>();

  // 如果是空树，初始化一个叶子页面作为顶部页面
  if (header_page->root_page_id_ == INVALID_PAGE_ID) {
    auto newpage = bpm_->NewPageGuarded(&header_page->root_page_id_);
    auto tmp = newpage.AsMut<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();
    tmp->Init(this->leaf_max_size_);
  }
  ctx.root_page_id_ = header_page->root_page_id_;

  WritePageGuard iter_pageguard = bpm_->FetchPageWrite(ctx.root_page_id_);
  auto iter_page = iter_pageguard.AsMut<BPlusTreePage>();

  while (!iter_page->IsLeafPage()) {
    auto tmp_inter_page = iter_pageguard.AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
    int i;
    i = this->KeyIndex(key, tmp_inter_page);
    // 如果i走到了最后没找到对应的key，那么说明该中间节点的所有的key都比待插入的key小，因此把最大的那个key改成要插入的key
    if (i == (tmp_inter_page->GetSize() - 1) && comparator_(tmp_inter_page->KeyAt(i), key) == -1) {
      tmp_inter_page->SetKeyAt(i, key);
    }

    // 将页面存到set中，并更新迭代
    ctx.write_set_.push_back(std::move(iter_pageguard));
    iter_pageguard = bpm_->FetchPageWrite(tmp_inter_page->ValueAt(i));
    iter_page = iter_pageguard.AsMut<BPlusTreePage>();
  }
  auto leaf_page = iter_pageguard.AsMut<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();

  int insert_pos = leaf_page->GetSize() != 0 ? this->KeyIndex(key, leaf_page) : 0;

  // 如果该key已经存在，返回false

  if (leaf_page->GetSize() != 0 && comparator_(key, leaf_page->KeyAt(insert_pos)) == 0) {
    return false;
  }
  if (leaf_page->GetSize() != 0 && comparator_(key, leaf_page->KeyAt(insert_pos)) == 1) {
    insert_pos = leaf_page->GetSize();
  }

  // 如果节点还能容下一个kv，则直接插入

  InsertKey(key, value, leaf_page, insert_pos);
  if (leaf_page->GetSize() < leaf_page->GetMaxSize()) {
    return true;
  }

  page_id_t child1 = iter_pageguard.PageId();
  page_id_t child2;

  auto newpage = bpm_->NewPageGuarded(&child2);
  auto tmp_leaf_page = newpage.AsMut<BPlusTreeLeafPage<KeyType, ValueType, KeyComparator>>();
  tmp_leaf_page->Init(this->leaf_max_size_);

  KeyType child1_key;
  KeyType child2_key;
  SplitNode(leaf_page, tmp_leaf_page);

  tmp_leaf_page->SetNextPageId(leaf_page->GetNextPageId());
  leaf_page->SetNextPageId(child2);

  child1_key = leaf_page->KeyAt(leaf_page->GetSize() - 1);
  child2_key = tmp_leaf_page->KeyAt(tmp_leaf_page->GetSize() - 1);

  newpage.Drop();
  iter_pageguard.Drop();

  while (!ctx.write_set_.empty()) {
    WritePageGuard tmp_inter_pageguard = std::move(ctx.write_set_.back());
    ctx.write_set_.pop_back();
    auto tmp_inter_page = tmp_inter_pageguard.AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();

    tmp_inter_page->SetValueAt(KeyIndex(child2_key, tmp_inter_page), child2);

    int insert_pos = KeyIndex(child1_key, tmp_inter_page);

    if (tmp_inter_page->GetSize() < tmp_inter_page->GetMaxSize()) {
      InsertKey(child1_key, child1, tmp_inter_page, insert_pos);
      return true;
    }

    newpage = bpm_->NewPageGuarded(&child2);
    auto tmp_page = newpage.AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
    tmp_page->Init(this->internal_max_size_);

    SplitNode(tmp_inter_page, tmp_page, insert_pos, child1_key, child1);
    child1 = tmp_inter_pageguard.PageId();
    child1_key = tmp_inter_page->KeyAt(tmp_inter_page->GetSize() - 1);
    child2_key = tmp_page->KeyAt(tmp_page->GetSize() - 1);

    newpage.Drop();
    tmp_inter_pageguard.Drop();
  }

  newpage = bpm_->NewPageGuarded(&ctx.root_page_id_);
  auto tmp_page = newpage.AsMut<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator>>();
  tmp_page->Init(this->internal_max_size_);
  InsertKey(child1_key, child1, tmp_page, 0);
  InsertKey(child2_key, child2, tmp_page, 1);
  header_page->root_page_id_ = newpage.PageId();
  (void)ctx;
  return true;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immediately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *txn) {
  // Declaration of context instance.
  Context ctx;
  (void)ctx;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() const -> page_id_t {
  auto headerpage = bpm_->FetchPageBasic(header_page_id_);
  const auto *header = headerpage.As<BPlusTreeHeaderPage>();
  return header->root_page_id_;
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, txn);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *txn) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, txn);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::PrintTree(page_id_t page_id, const BPlusTreePage *page) {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    std::cout << "Leaf Page: " << page_id << "\tNext: " << leaf->GetNextPageId() << std::endl;

    // Print the contents of the leaf page.
    std::cout << "Contents: ";
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i);
      if ((i + 1) < leaf->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;

  } else {
    auto *internal = reinterpret_cast<const InternalPage *>(page);
    std::cout << "Internal Page: " << page_id << std::endl;

    // Print the contents of the internal page.
    std::cout << "Contents: ";
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i);
      if ((i + 1) < internal->GetSize()) {
        std::cout << ", ";
      }
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      auto guard = bpm_->FetchPageBasic(internal->ValueAt(i));
      PrintTree(guard.PageId(), guard.template As<BPlusTreePage>());
    }
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Drawing an empty tree");
    return;
  }

  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  auto root_page_id = GetRootPageId();
  auto guard = bpm->FetchPageBasic(root_page_id);
  ToGraph(guard.PageId(), guard.template As<BPlusTreePage>(), out);
  out << "}" << std::endl;
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(page_id_t page_id, const BPlusTreePage *page, std::ofstream &out) {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<const LeafPage *>(page);
    // Print node name
    out << leaf_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << page_id << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << page_id << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }
  } else {
    auto *inner = reinterpret_cast<const InternalPage *>(page);
    // Print node name
    out << internal_prefix << page_id;
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << page_id << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_guard = bpm_->FetchPageBasic(inner->ValueAt(i));
      auto child_page = child_guard.template As<BPlusTreePage>();
      ToGraph(child_guard.PageId(), child_page, out);
      if (i > 0) {
        auto sibling_guard = bpm_->FetchPageBasic(inner->ValueAt(i - 1));
        auto sibling_page = sibling_guard.template As<BPlusTreePage>();
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_guard.PageId() << " " << internal_prefix
              << child_guard.PageId() << "};\n";
        }
      }
      out << internal_prefix << page_id << ":p" << child_guard.PageId() << " -> ";
      if (child_page->IsLeafPage()) {
        out << leaf_prefix << child_guard.PageId() << ";\n";
      } else {
        out << internal_prefix << child_guard.PageId() << ";\n";
      }
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::DrawBPlusTree() -> std::string {
  if (IsEmpty()) {
    return "()";
  }

  PrintableBPlusTree p_root = ToPrintableBPlusTree(GetRootPageId());
  std::ostringstream out_buf;
  p_root.Print(out_buf);

  return out_buf.str();
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::ToPrintableBPlusTree(page_id_t root_id) -> PrintableBPlusTree {
  auto root_page_guard = bpm_->FetchPageBasic(root_id);
  auto root_page = root_page_guard.template As<BPlusTreePage>();
  PrintableBPlusTree proot;

  if (root_page->IsLeafPage()) {
    auto leaf_page = root_page_guard.template As<LeafPage>();
    proot.keys_ = leaf_page->ToString();
    proot.size_ = proot.keys_.size() + 4;  // 4 more spaces for indent

    return proot;
  }

  // draw internal page
  auto internal_page = root_page_guard.template As<InternalPage>();
  proot.keys_ = internal_page->ToString();
  proot.size_ = 0;
  for (int i = 0; i < internal_page->GetSize(); i++) {
    page_id_t child_id = internal_page->ValueAt(i);
    PrintableBPlusTree child_node = ToPrintableBPlusTree(child_id);
    proot.size_ += child_node.size_;
    proot.children_.push_back(child_node);
  }

  return proot;
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;

template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;

template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;

template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;

template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
