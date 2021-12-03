import EncryptedStorage from 'react-native-encrypted-storage';
import _ from 'lodash';

const deviceStorage = {
  keys: {},
  /**
	 * Get a one or more value for a key or array of keys from AsyncStorage
	 * @param {String|Array} key A key or array of keys
	 * @return {Promise}
	 */
  get: async (key) => {
    if(!Array.isArray(key)) {
      return await EncryptedStorage.getItem(key).then(value => {
        return JSON.parse(value);
      });
    }
    else {
      let result = [];
      for (const k of key) {
        const value = await EncryptedStorage.getItem(k);
        result.push(JSON.parse(value));
      }
      return result;
    }
  },

  /**
	 * Save a key value pair or a series of key value pairs to AsyncStorage.
	 * @param  {String|Array} key The key or an array of key/value pairs
	 * @param  {Any} value The value to save
	 * @return {Promise}
	 */
  save: async (key, value) => {
    if(!Array.isArray(key)) {
      this.default.keys[key] = 1
      return await EncryptedStorage.setItem(key, JSON.stringify(value));
    } else {
      await key.map(async (pair) => {
        this.default.keys[pair[0]] = 1;
        await EncryptedStorage.setItem(pair[0], JSON.stringify(pair[1]));
      });
      return [];
    }
  },

  /**
	 * Delete the value for a given key in AsyncStorage.
	 * @param  {String|Array} key The key or an array of keys to be deleted
	 * @return {Promise}
	 */
  delete: async (key) => {
    if (Array.isArray(key)) {
      let res = [];
      for (const k of key) {
        delete this.default.keys[k]
        const r = await EncryptedStorage.removeItem(k);
        res.push(r);
      }
      return res;
    } else {
      delete this.default.keys[key]
      return await EncryptedStorage.removeItem(key);
    }
  },

  /**
	 * Push a value onto an array stored in AsyncStorage by key or create a new array in AsyncStorage for a key if it's not yet defined.
	 * @param {String} key They key
	 * @param {Any} value The value to push onto the array
	 * @return {Promise}
	 */
  push: async (key, value) => {
    return deviceStorage.get(key).then((currentValue) => {
      if (currentValue === null) {
        // if there is no current value populate it with the new value
        return deviceStorage.save(key, [value]);
      }
      if (Array.isArray(currentValue)) {
        currentValue.push(value)
        return deviceStorage.save(key, currentValue);
      }
      throw new Error(`Existing value for key "${key}" must be of type null or Array, received ${typeof currentValue}.`);
    });
  },

  /**
   * Gets all keys known to the app, for all callers, libraries, etc
   * @return the array of keys in the storage
   */
  getKeys: async (prefix = null) => {
    let result = Object.keys(this.default.keys);
    if(prefix){
      result = result.filter((e)=>e.startsWith(prefix));
    }
    return result;
  },

  /**
   * Deletes all the items in the storage.
   */
  deleteAll: async ()=>{
    this.default.keys = {};
    await EncryptedStorage.clear();
  }
};

_.bindAll(deviceStorage);

export default deviceStorage;
