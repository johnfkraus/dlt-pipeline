


alphabet_str = "a b c d e f g h i j k l m n o p q r s t u v w x y z"
alphabet_list = alphabet_str.split()
vowels = "a e i o u"
vowel_list = vowels.split()

print(f"{alphabet_list} - {vowel_list} = \n{sorted((set(alphabet_list) - set(vowel_list)))}")

diff = [x for x in alphabet_list if x not in vowel_list]
print(diff)

