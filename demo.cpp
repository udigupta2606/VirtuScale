#include <iostream>
#include <vector>
#include <cstring>
#include <cassert>

using namespace std;

constexpr int MAX_KEYS = 4;

struct KeyValue {
    int key;
    string value;
};

struct BTreeNode {
    bool isLeaf;
    int count;
    vector<int> keys;
    vector<string> values;            // Only used if isLeaf
    vector<BTreeNode*> children;      // Only used if !isLeaf
    BTreeNode* nextLeaf;

    BTreeNode(bool leaf) : isLeaf(leaf), count(0), nextLeaf(nullptr) {}

    void print() {
        cout << (isLeaf ? "[Leaf] " : "[Inner] ") << "Keys: ";
        for (int k : keys) cout << k << " ";
        cout << "\n";
    }
};

class BPlusTree {
    BTreeNode* root;

public:
    BPlusTree() {
        root = new BTreeNode(true);
    }

    void insert(int key, const string& value) {
        if (root->count == MAX_KEYS) {
            BTreeNode* newRoot = new BTreeNode(false);
            newRoot->children.push_back(root);
            splitChild(newRoot, 0);
            root = newRoot;
        }
        insertNonFull(root, key, value);
    }

    string lookup(int key) {
        BTreeNode* node = root;
        while (!node->isLeaf) {
            int i = 0;
            while (i < node->count && key >= node->keys[i]) i++;
            node = node->children[i];
        }
        for (int i = 0; i < node->count; ++i) {
            if (node->keys[i] == key)
                return node->values[i];
        }
        return "NOT_FOUND";
    }

    void traverse() {
        BTreeNode* node = root;
        while (!node->isLeaf)
            node = node->children[0];

        while (node) {
            for (int i = 0; i < node->count; i++)
                cout << node->keys[i] << ":" << node->values[i] << " ";
            node = node->nextLeaf;
        }
        cout << "\n";
    }

private:
    void insertNonFull(BTreeNode* node, int key, const string& value) {
        if (node->isLeaf) {
            int i = node->count - 1;
            node->keys.push_back(0);
            node->values.push_back("");
            while (i >= 0 && node->keys[i] > key) {
                node->keys[i + 1] = node->keys[i];
                node->values[i + 1] = node->values[i];
                i--;
            }
            node->keys[i + 1] = key;
            node->values[i + 1] = value;
            node->count++;
        } else {
            int i = node->count - 1;
            while (i >= 0 && key < node->keys[i])
                i--;
            i++;
            if (node->children[i]->count == MAX_KEYS) {
                splitChild(node, i);
                if (key > node->keys[i])
                    i++;
            }
            insertNonFull(node->children[i], key, value);
        }
    }

    void splitChild(BTreeNode* parent, int i) {
        BTreeNode* fullNode = parent->children[i];
        BTreeNode* newNode = new BTreeNode(fullNode->isLeaf);

        int mid = MAX_KEYS / 2;
        newNode->count = MAX_KEYS - mid - 1;

        parent->keys.insert(parent->keys.begin() + i, fullNode->keys[mid]);
        parent->children.insert(parent->children.begin() + i + 1, newNode);
        parent->count++;

        if (fullNode->isLeaf) {
            for (int j = mid; j < MAX_KEYS; j++) {
                newNode->keys.push_back(fullNode->keys[j]);
                newNode->values.push_back(fullNode->values[j]);
            }
            fullNode->keys.resize(mid);
            fullNode->values.resize(mid);
            fullNode->count = mid;

            newNode->nextLeaf = fullNode->nextLeaf;
            fullNode->nextLeaf = newNode;
        } else {
            for (int j = mid + 1; j < MAX_KEYS; j++) {
                newNode->keys.push_back(fullNode->keys[j]);
            }
            for (int j = mid + 1; j <= MAX_KEYS; j++) {
                newNode->children.push_back(fullNode->children[j]);
            }
            fullNode->keys.resize(mid);
            fullNode->children.resize(mid + 1);
            fullNode->count = mid;
        }
    }
};