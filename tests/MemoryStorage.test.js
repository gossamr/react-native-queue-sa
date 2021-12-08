/* global describe it beforeEach process jest expect*/

import { memoryStorage as storage } from '../Models/MemoryStorage';

beforeEach(async () => {
  storage.deleteAll();
});

it('can store a value', async ()=>{
  let object = {id: 'obj'};
  storage.save('value', object);

  let result = await storage.get('value');
  expect(result).toMatchObject(object);
});

it('can store an array', async ()=>{
  let array = [{id: 'a'}, {id: 'b'}, {id: 'c'}];
  let pairs = array.map((el)=> [el.id, el]);
  let ids = array.map((el)=>el.id);

  storage.save(pairs);

  let result = await storage.get(ids);
  expect(result).toMatchObject(array);
});

function setupThreeItems(){
  let array = [{id: 'a'}, {id: 'b'}, {id: 'c'}];
  let pairs = array.map((el)=> [el.id, el]);

  storage.save(pairs);
  return array.map((el)=>el.id);
}

it('can list the keys', async()=>{
  let ids = setupThreeItems();

  let result = await storage.getKeys();
  expect(result).toMatchObject(ids);
});

it('can delete an item', async()=>{
  let ids = setupThreeItems();
  await storage.delete(ids[0]);
  let result = await storage.getKeys();
  ids.splice(0, 1);
  expect(result).toMatchObject(ids);
});

it('can delete all items', async()=>{
  setupThreeItems();
  await storage.deleteAll();
  let result = await storage.getKeys();

  expect(result.length).toBe(0);
});

it('can push an item', async()=>{
  let array = [{id: 'a'}, {id: 'b'}, {id: 'c'}];
  for(let i = 0; i < array.length; i++){
    let a = array[i];
    await storage.push('array', a);
  }
  let result = await storage.get('array');
  expect(result).toMatchObject(array);
});
