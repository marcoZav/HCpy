import pickle



f = open('C:/Users/itamrz/OneDrive - SAS/Dati/Snam/Cloud/pickle/Apollo_Termoelettrico_Marzo23.pkl', 'rb')    
oo = pickle.load(f)

print(oo)
print(oo.boosting_type)
print(oo.objective )
print(oo.class_weight)